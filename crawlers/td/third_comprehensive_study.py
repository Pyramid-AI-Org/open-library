from __future__ import annotations

from html.parser import HTMLParser
import random
import re

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
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
        self.title: str = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "h1" and not self.h1:
            self._capture_tag = "h1"
            self._parts = []
        elif t == "h2" and not self.h2:
            self._capture_tag = "h2"
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
            elif self._capture_tag == "title" and text:
                self.title = text
            self._capture_tag = None
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_tag:
            self._parts.append(data)


def _normalize_heading(text: str) -> str:
    heading = clean_text(text)
    free_copy_token = "*Free copy can only be downloaded from this website"
    idx = heading.lower().find(free_copy_token.lower())
    if idx >= 0:
        heading = clean_text(heading[:idx])
    # Some TD pages include a site label prefix in <title>; keep only document title.
    heading = re.sub(r"^\s*transport\s+department\s*[:-]\s*", "", heading, flags=re.IGNORECASE)
    return heading


def _extract_page_title(html: str) -> str:
    parser = _HeadingParser()
    parser.feed(html or "")
    return _normalize_heading(parser.h1 or parser.h2 or parser.title)


class Crawler:
    name = "third_comprehensive_study"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        page_url = str(
            cfg.get(
                "page_url",
                "https://www.td.gov.hk/en/publications_and_press_releases/publications/free_publications/the_third_comprehensive_transport_study/index.html",
            )
        ).strip()
        publish_date = str(cfg.get("publish_date", "1999-01-01")).strip()
        chapter_path_prefix = str(
            cfg.get(
                "chapter_path_prefix",
                "/en/publications_and_press_releases/publications/free_publications/the_third_comprehensive_transport_study/",
            )
        ).strip()

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
            if can_url == page_url:
                continue

            lower_url = can_url.lower()
            if chapter_path_prefix and chapter_path_prefix not in lower_url:
                continue
            if not lower_url.endswith("/index.html"):
                continue
            if can_url in seen_urls:
                continue

            seen_urls.add(can_url)
            chapter_name = clean_text(link.text) or infer_name_from_link(None, can_url)
            name = f"{page_title} - {chapter_name}" if page_title else chapter_name

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
