from __future__ import annotations

from html.parser import HTMLParser
import random

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
from utils.html_links import extract_links


class _HeadingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_tag: str | None = None
        self._parts: list[str] = []
        self.h1: str = ""
        self.h2: str = ""
        self.h4: str = ""
        self.title: str = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "h1" and not self.h1:
            self._capture_tag = "h1"
            self._parts = []
        elif t == "h2" and not self.h2:
            self._capture_tag = "h2"
            self._parts = []
        elif t == "h4" and not self.h4:
            self._capture_tag = "h4"
            self._parts = []
        elif t == "title" and not self.title:
            self._capture_tag = "title"
            self._parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if self._capture_tag and t == self._capture_tag:
            text = clean_text("".join(self._parts))
            if self._capture_tag == "h1" and text:
                self.h1 = text
            elif self._capture_tag == "h2" and text:
                self.h2 = text
            elif self._capture_tag == "h4" and text:
                self.h4 = text
            elif self._capture_tag == "title" and text:
                self.title = text
            self._capture_tag = None
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_tag:
            self._parts.append(data)


def _extract_page_title(html: str) -> str:
    parser = _HeadingParser()
    parser.feed(html or "")
    return clean_text(parser.h1 or parser.h2 or parser.h4 or parser.title)


class Crawler:
    name = "transport_planning"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        page_url = str(
            cfg.get(
                "page_url",
                "https://www.td.gov.hk/en/publications_and_press_releases/publications/tpdm/index.html",
            )
        ).strip()
        publish_date = str(cfg.get("publish_date", "2026-03-01")).strip()

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay_seconds > 0:
            sleep_seconds(
                request_delay_seconds
                + random.uniform(0.0, max(0.0, request_jitter_seconds))
            )

        resp = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )
        resp.encoding = "utf-8"
        html = resp.text or ""

        page_title = _extract_page_title(html)
        links = extract_links(html, base_url=page_url)

        seen_urls: set[str] = set()
        out: list[UrlRecord] = []

        for link in links:
            can_url = canonicalize_url(link.href, encode_spaces=True)
            if not can_url:
                continue
            if path_ext(can_url) != ".pdf":
                continue
            if can_url in seen_urls:
                continue

            seen_urls.add(can_url)
            pdf_name = clean_text(link.text) or infer_name_from_link(None, can_url)
            name = f"{page_title} - {pdf_name}" if page_title else pdf_name

            out.append(
                ctx.make_record(
                    url=can_url,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    publish_date=publish_date,
                    meta={
                        "discovered_from": page_url,
                    },
                )
            )

        out.sort(key=lambda r: (r.name or "", r.url))
        return out
