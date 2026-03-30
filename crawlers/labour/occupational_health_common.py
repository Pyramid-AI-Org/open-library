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


class _OccupationalHealthPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self._in_main_content = False
        self._main_content_depth = 0

        self._li_depth = 0
        self._current_li_text_parts: list[str] = []
        self._current_li_links: list[tuple[str, str]] = []
        self._current_li_options: list[tuple[str, str]] = []

        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

        self._in_option = False
        self._option_value: str | None = None
        self._option_text_parts: list[str] = []

        self.items: list[dict[str, object]] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    def _reset_current_li(self) -> None:
        self._current_li_text_parts = []
        self._current_li_links = []
        self._current_li_options = []

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
            if self._li_depth == 0:
                self._reset_current_li()
            self._li_depth += 1
            return

        if self._li_depth <= 0:
            return

        if t == "a":
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []
            return

        if t == "option":
            self._in_option = True
            self._option_value = attrs_map.get("value")
            self._option_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._in_main_content:
            self._main_content_depth -= 1
            if self._main_content_depth <= 0:
                self._in_main_content = False
                self._main_content_depth = 0

        if not self._in_main_content:
            return

        if t == "a" and self._in_a:
            href = clean_text(self._a_href)
            if href:
                self._current_li_links.append(
                    (href, clean_text("".join(self._a_text_parts)))
                )
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            return

        if t == "option" and self._in_option:
            value = clean_text(self._option_value)
            if value:
                option_text = clean_text("".join(self._option_text_parts))
                self._current_li_options.append((value, option_text))
            self._in_option = False
            self._option_value = None
            self._option_text_parts = []
            return

        if t != "li" or self._li_depth <= 0:
            return

        self._li_depth -= 1
        if self._li_depth > 0:
            return

        self.items.append(
            {
                "text": clean_text("".join(self._current_li_text_parts)),
                "links": list(self._current_li_links),
                "options": list(self._current_li_options),
            }
        )
        self._reset_current_li()

    def handle_data(self, data: str) -> None:
        if not self._in_main_content or self._li_depth <= 0:
            return

        self._current_li_text_parts.append(data)

        if self._in_a:
            self._a_text_parts.append(data)
        if self._in_option:
            self._option_text_parts.append(data)


_NON_ENGLISH_LANGUAGE_NAMES = {
    "chinese",
    "hindi",
    "nepali",
    "tagalog",
    "thai",
    "urdu",
    "indonesian",
    "philippine",
    "bahasa",
    "bahasa indonesia",
    "bahasa indonesian",
}
_LANGUAGE_LABELS = _NON_ENGLISH_LANGUAGE_NAMES | {"english"}

_CJK_LANGUAGE_TOKENS = {
    "中文",
    "英文",
    "印度文",
    "尼泊爾文",
    "菲律賓文",
    "泰文",
    "巴基斯坦文",
    "印尼文",
}


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
    cleaned = re.sub(
        r"\(\s*[\d.,]+\s*(?:KB|MB|GB)\s*\)", "", cleaned, flags=re.IGNORECASE
    )
    return clean_text(cleaned)


def _is_non_english_language_label(text: str) -> bool:
    return _normalize_link_text(text).lower() in _NON_ENGLISH_LANGUAGE_NAMES


def _is_language_label(text: str) -> bool:
    return _normalize_link_text(text).lower() in _LANGUAGE_LABELS


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _is_pdf_url(url: str) -> bool:
    return path_ext(url) == ".pdf"


def _is_chinese_only(text: str) -> bool:
    cleaned = clean_text(text)
    if not cleaned:
        return False
    has_cjk = any("\u4e00" <= ch <= "\u9fff" for ch in cleaned)
    has_latin = any(("a" <= ch.lower() <= "z") for ch in cleaned)
    return has_cjk and not has_latin


def _infer_locale(url: str, *, name_text: str | None = None) -> str:
    lower_url = (url or "").lower()

    if "/tc/" in lower_url or "/tc_chi/" in lower_url:
        return "tc"
    if _is_chinese_only(name_text or ""):
        return "tc"
    return "en"


def _looks_like_name(value: str) -> bool:
    normalized = _normalize_link_text(value)
    if not normalized:
        return False

    if normalized.lower() in _LANGUAGE_LABELS | {"open"}:
        return False

    if normalized in _CJK_LANGUAGE_TOKENS:
        return False

    return True


def _derive_base_name(li_text: str) -> str:
    text = clean_text(li_text)
    if not text:
        return ""

    text = re.sub(
        r"\(\s*please\s+select\s+language[^\)]*\)",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\(\s*pdf\s*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bopen\b\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"--\s*language\s*--", "", text, flags=re.IGNORECASE)

    for token in _CJK_LANGUAGE_TOKENS:
        idx = text.find(token)
        if idx > 0:
            text = text[:idx]
            break

    m = re.search(
        r"\b(?:English|Chinese|Bahasa|Hindi|Nepali|Tagalog|Thai|Urdu|Indonesian|Philippine)\b",
        text,
    )
    if m and m.start() > 0:
        text = text[: m.start()]

    return clean_text(text)


def crawl_occupational_health_part(
    *,
    ctx: RunContext,
    crawler_name: str,
    default_page_url_en: str,
    default_page_url_tc: str,
) -> list[UrlRecord]:
    cfg = ctx.get_crawler_config(crawler_name)

    page_url_en = str(cfg.get("page_url_en", default_page_url_en)).strip()
    page_url_tc = str(cfg.get("page_url_tc", default_page_url_tc)).strip()
    crawl_tc_page = bool(cfg.get("crawl_tc_page", False))

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

    listings: list[str] = [page_url_en]
    if crawl_tc_page and page_url_tc:
        listings.append(page_url_tc)

    out: list[UrlRecord] = []
    seen_urls: set[str] = set()

    for listing_url in listings:
        if len(out) >= max_total_records:
            break

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        try:
            response = get_with_retries(
                session,
                listing_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
        except requests.RequestException:
            continue

        response.encoding = "utf-8"
        parser = _OccupationalHealthPageParser()
        parser.feed(response.text or "")

        for item in parser.items:
            if len(out) >= max_total_records:
                break

            li_text = clean_text(str(item.get("text", "")))
            links = item.get("links", [])
            options = item.get("options", [])

            if not isinstance(links, list) or not isinstance(options, list):
                continue

            base_name = _derive_base_name(li_text)

            entries: list[tuple[str, str, str | None]] = []

            for href, link_text in links:
                if not isinstance(href, str):
                    continue

                if _is_non_english_language_label(link_text or ""):
                    continue

                abs_url = _canonicalize(urljoin(listing_url, href))
                if not abs_url or not _is_pdf_url(abs_url):
                    continue

                chosen_name = (
                    clean_text(link_text) if isinstance(link_text, str) else ""
                )
                if not _looks_like_name(chosen_name):
                    chosen_name = base_name

                locale = _infer_locale(abs_url, name_text=chosen_name or base_name)

                entries.append((abs_url, locale, chosen_name or None))

            for value, option_text in options:
                if not isinstance(value, str):
                    continue

                if _is_non_english_language_label(option_text or ""):
                    continue

                abs_url = _canonicalize(urljoin(listing_url, value))
                if not abs_url or not _is_pdf_url(abs_url):
                    continue

                locale = _infer_locale(abs_url, name_text=base_name or option_text)

                entries.append((abs_url, locale, base_name or None))

            for abs_url, locale, raw_name in entries:
                if len(out) >= max_total_records:
                    break
                if abs_url in seen_urls:
                    continue

                name = clean_text(raw_name) if isinstance(raw_name, str) else ""
                if not name:
                    name = infer_name_from_link(None, abs_url) or ""

                out.append(
                    ctx.make_record(
                        url=abs_url,
                        name=name or None,
                        discovered_at_utc=ctx.run_date_utc,
                        source=crawler_name,
                        meta={
                            "discovered_from": listing_url,
                            "locale": locale,
                        },
                    )
                )
                seen_urls.add(abs_url)

    out.sort(
        key=lambda r: (
            r.url,
            str(r.name or ""),
            str(r.meta.get("locale") or ""),
            str(r.meta.get("discovered_from") or ""),
        )
    )
    return out
