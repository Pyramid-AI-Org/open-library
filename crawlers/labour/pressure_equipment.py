from __future__ import annotations

import random
import re
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)


_DEFAULT_PAGE_URL = "https://www.labour.gov.hk/eng/public/content2_10.htm"
_NON_ENGLISH_LANGUAGE_NAMES = {
    "chinese",
    "hindi",
    "nepali",
    "urdu",
    "thai",
    "tagalog",
    "indonesian",
    "bahasa",
}
_LANGUAGE_LABELS = _NON_ENGLISH_LANGUAGE_NAMES | {"english"}


class _PressureEquipmentPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str, str, str]] = []

        self._in_main_content = False
        self._main_content_depth = 0

        self._li_text_stack: list[list[str]] = []

        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            if self._in_main_content:
                self._main_content_depth += 1
            elif attrs_map.get("id", "").strip().lower() == "maincontent":
                self._in_main_content = True
                self._main_content_depth = 1

        if not self._in_main_content:
            return

        if t == "li":
            self._li_text_stack.append([])
            return

        if t == "a":
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._in_main_content:
            self._main_content_depth -= 1
            if self._main_content_depth <= 0:
                self._in_main_content = False
                self._main_content_depth = 0

        if not self._in_main_content:
            return

        if t == "li" and self._li_text_stack:
            self._li_text_stack.pop()
            return

        if t == "a" and self._in_a:
            href = clean_text(self._a_href)
            if href:
                link_text = clean_text("".join(self._a_text_parts))
                li_text = (
                    clean_text("".join(self._li_text_stack[-1]))
                    if self._li_text_stack
                    else ""
                )
                parent_li_text = (
                    clean_text("".join(self._li_text_stack[-2]))
                    if len(self._li_text_stack) >= 2
                    else ""
                )
                self.links.append((href, link_text, li_text, parent_li_text))
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_main_content and self._li_text_stack:
            self._li_text_stack[-1].append(data)

        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _is_pdf_url(url: str) -> bool:
    return path_ext(url) == ".pdf"


def _is_english_file(*, url: str, name: str) -> bool:
    lower_name = clean_text(name).lower()
    if lower_name in _NON_ENGLISH_LANGUAGE_NAMES:
        return False

    return True


def _is_tc_url(url: str) -> bool:
    lower_url = (url or "").lower()
    return "/tc/" in lower_url or "/tc_chi/" in lower_url


def _has_chinese_only_marker(text: str) -> bool:
    return bool(re.search(r"\(\s*c\s*\)", text or "", flags=re.IGNORECASE))


def _looks_like_chinese_only_pdf(url: str) -> bool:
    return (url or "").lower().endswith("_c.pdf")


def _normalize_link_text(text: str) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""

    cleaned = re.sub(
        r"this\s*link\s*will\s*open\s*in\s*a\s*new\s*window",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\bpdf\b", "", cleaned, flags=re.IGNORECASE)
    return clean_text(cleaned)


def _is_language_label(text: str) -> bool:
    normalized = _normalize_link_text(text).lower()
    return normalized in _LANGUAGE_LABELS


def _language_label(text: str) -> str:
    normalized = _normalize_link_text(text).lower()
    return normalized if normalized in _LANGUAGE_LABELS else ""


def _derive_name_from_li_text(text: str) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""

    language_tokens = (
        "English",
        "Chinese",
        "Hindi",
        "Nepali",
        "Urdu",
        "Thai",
        "Tagalog",
        "Indonesian",
        "Bahasa",
    )
    for token in language_tokens:
        idx = cleaned.find(token)
        if idx > 0:
            cleaned = cleaned[:idx]
            break
    return clean_text(cleaned)


class Crawler:
    name = "pressure_equipment"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 60))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        response = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"

        parser = _PressureEquipmentPageParser()
        parser.feed(response.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for href, link_text, li_text, parent_li_text in parser.links:
            if len(out) >= max_total_records:
                break

            abs_url = _canonicalize(urljoin(response.url, href))
            if not abs_url or not _is_pdf_url(abs_url):
                continue

            language_label = _language_label(link_text)
            if language_label in _NON_ENGLISH_LANGUAGE_NAMES:
                continue

            # Keep Chinese-only publications explicitly marked as (C) on the EN listing.
            if _is_tc_url(abs_url) and not (
                _has_chinese_only_marker(li_text)
                or _looks_like_chinese_only_pdf(abs_url)
            ):
                continue

            name = _normalize_link_text(link_text)
            if not name or language_label == "english" or _is_language_label(name):
                parent_name = _derive_name_from_li_text(parent_li_text)
                current_name = _derive_name_from_li_text(li_text)
                name = (
                    parent_name or current_name or infer_name_from_link(None, abs_url)
                )
            if not name or not _is_english_file(url=abs_url, name=name):
                continue

            if abs_url in seen_urls:
                continue
            seen_urls.add(abs_url)

            out.append(
                ctx.make_record(
                    url=abs_url,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "locale": "tc" if _is_tc_url(abs_url) else "en",
                        "discovered_from": page_url,
                    },
                )
            )

        return out
