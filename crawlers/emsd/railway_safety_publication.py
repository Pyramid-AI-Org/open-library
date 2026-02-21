from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    get_with_retries,
    sleep_seconds,
)

logger = logging.getLogger(__name__)


@dataclass
class _RowData:
    pdf_url: str | None = None
    title: str | None = None
    category: str | None = None
    date: str | None = None


class _TableParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.rows: list[_RowData] = []

        self._in_table = False
        self._in_tr = False
        self._current_col = -1  # Renamed from _col_idx to match logic

        self._current_row: _RowData | None = None
        self._td_buffer: list[str] = []  # Renamed from _buffer
        self._in_a = False
        self._a_buffer: list[str] = []  # Renamed from _current_a_text
        self._current_href: str | None = None  # Renamed from _current_a_href

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table":
            self._in_table = True

        elif tag == "tr":
            if self._in_table:
                self._in_tr = True
                self._current_row = _RowData()
                self._current_col = -1

        elif tag == "td":
            if self._in_tr:
                self._current_col += 1
                self._td_buffer = []

        elif tag == "a":
            # Only care about links inside TD
            if self._in_tr and self._current_col >= 0:
                self._in_a = True
                self._a_buffer = []
                for k, v in attrs:
                    if k == "href":
                        self._current_href = v
                        break

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "table":
            self._in_table = False

        elif tag == "tr":
            if self._in_tr:
                self._in_tr = False
                # Row finished. If we found a PDF URL, keep it.
                if self._current_row and self._current_row.pdf_url:
                    self.rows.append(self._current_row)
                self._current_row = None

        elif tag == "td":
            if self._in_tr:
                # Column finished.
                text = "".join(self._td_buffer).strip()
                if self._current_row:
                    if self._current_col == 1:  # Category
                        self._current_row.category = text
                    elif self._current_col == 2:  # Date
                        self._current_row.date = text

                self._td_buffer = []

        elif tag == "a":
            if self._in_a:
                self._in_a = False
                # If inside Col 0 (Publication), extract PDF URL and Title
                if self._current_row and self._current_col == 0:
                    href = self._current_href
                    link_text = "".join(self._a_buffer).strip()

                    if href:
                        abs_url = urljoin(self.base_url, href)
                        parsed = urlparse(abs_url)
                        if parsed.path.lower().endswith(".pdf"):
                            self._current_row.pdf_url = abs_url
                            # Use link text as title if present
                            if link_text and not self._current_row.title:
                                self._current_row.title = link_text

                self._current_href = None
                self._a_buffer = []

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._a_buffer.append(data)

        # Capture TD text only if NOT inside an A tag to avoid duplication?
        # Actually standard practice is to capture everything for the cell text.
        # But for Category/Date columns, usually no links.
        # For Publication column, we only care about the link text.
        if self._in_tr and self._current_col >= 0:
            self._td_buffer.append(data)


class Crawler:
    name = "emsd.railway_safety_publication"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        crawler_cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        page_url = str(
            crawler_cfg.get(
                "page_url",
                "https://www.emsd.gov.hk/en/railway_safety/publication/index.html",
            )
        ).strip()

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(crawler_cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(crawler_cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(crawler_cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(crawler_cfg.get("backoff_jitter_seconds", 0.25))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0, request_jitter))

        try:
            if ctx.debug:
                logger.info(f"[{self.name}] Fetching {page_url}")
            resp = get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
        except Exception as e:
            logger.error(f"Failed to fetch {page_url}: {e}")
            return []

        # Parse HTML
        parser = _TableParser(base_url=page_url)
        parser.feed(resp.text)

        records = []
        for row in parser.rows:
            if not row.pdf_url:
                continue

            # Use filename as fallback title if empty
            title = row.title
            if not title:
                parsed = urlparse(row.pdf_url)
                title = parsed.path.split("/")[-1]

            records.append(
                UrlRecord(
                    url=row.pdf_url,
                    name=title,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={
                        "discovered_from": page_url,
                        "category": row.category,
                        "date": row.date,
                    },
                )
            )

        logger.info(f"Found {len(records)} PDF records from {page_url}")
        return records
