from __future__ import annotations

import random
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    normalize_publish_date,
    path_ext,
    sleep_seconds,
)


_DEFAULT_PAGE_URL = "https://www.hkfsd.gov.hk/eng/fire_protection/notices/code.html"
_DEFAULT_PDF_SCOPE_PREFIX = "https://www.hkfsd.gov.hk/"


@dataclass(frozen=True)
class _ParagraphData:
    text: str
    links: list[tuple[str, str]]


class _AccessParagraphParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.paragraphs: list[_ParagraphData] = []

        self._in_access = False
        self._access_depth = 0

        self._in_p = False
        self._p_text_parts: list[str] = []
        self._p_links: list[tuple[str, str]] = []

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

    @staticmethod
    def _classes(value: str) -> set[str]:
        return {part.strip().lower() for part in value.split() if part.strip()}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            classes = self._classes(attrs_map.get("class", ""))
            if self._in_access:
                self._access_depth += 1
                return
            if "access" in classes:
                self._in_access = True
                self._access_depth = 1
            return

        if not self._in_access:
            return

        if t == "p":
            self._in_p = True
            self._p_text_parts = []
            self._p_links = []
            return

        if t == "a" and self._in_p:
            href = clean_text(str(attrs_map.get("href") or ""))
            self._in_a = True
            self._a_href = href or None
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._in_access:
            self._access_depth -= 1
            if self._access_depth <= 0:
                self._in_access = False
                self._access_depth = 0
            return

        if not self._in_access:
            return

        if t == "a" and self._in_a:
            link_text = clean_text("".join(self._a_text_parts))
            if self._a_href:
                self._p_links.append((self._a_href, link_text))
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            return

        if t == "p" and self._in_p:
            text = clean_text("".join(self._p_text_parts))
            if text or self._p_links:
                self.paragraphs.append(_ParagraphData(text=text, links=self._p_links))
            self._in_p = False
            self._p_text_parts = []
            self._p_links = []

    def handle_data(self, data: str) -> None:
        if self._in_p:
            self._p_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "code_of_practice"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        pdf_scope_prefix = str(cfg.get("pdf_scope_prefix", _DEFAULT_PDF_SCOPE_PREFIX)).strip()

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
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

        parser = _AccessParagraphParser()
        parser.feed(response.text or "")

        latest_para: _ParagraphData | None = None
        for para in parser.paragraphs:
            if not para.links:
                continue
            if "current edition" in para.text.lower():
                latest_para = para
                break

        if latest_para is None:
            for para in parser.paragraphs:
                if para.links:
                    latest_para = para
                    break

        if latest_para is None:
            return []

        chosen_url: str | None = None
        chosen_link_text = ""
        for href, link_text in latest_para.links:
            candidate = _canonicalize(urljoin(page_url, href))
            if not candidate:
                continue
            if path_ext(candidate) != ".pdf":
                continue
            if pdf_scope_prefix and not candidate.startswith(pdf_scope_prefix):
                continue
            chosen_url = candidate
            chosen_link_text = clean_text(link_text)
            break

        if not chosen_url:
            return []

        name = clean_text(latest_para.text) or chosen_link_text or "Code of Practice"
        publish_date = normalize_publish_date(latest_para.text)

        return [
            ctx.make_record(
                url=chosen_url,
                name=name,
                discovered_at_utc=ctx.run_date_utc,
                source=self.name,
                meta={"discovered_from": page_url},
                publish_date=publish_date,
            )
        ]
