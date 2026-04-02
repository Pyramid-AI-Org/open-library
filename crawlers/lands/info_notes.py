from __future__ import annotations

import random
import re
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
    path_ext,
    sleep_seconds,
)

_DEFAULT_PAGE_URL = "https://www.landsd.gov.hk/en/resources/practice-notes/info-notes.html"


@dataclass(frozen=True)
class _RowData:
    year: str
    topic: str
    subject: str
    href: str | None


class _InfoNotesTableParser(HTMLParser):
    """Extract rows from the Information Notes table (Year/Topic/Subject)."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)

        self._in_table = False
        self._table_depth = 0
        self._table_matches = False

        self._in_tr = False
        self._in_th = False
        self._in_td = False
        self._td_index = 0

        self._current_cell_text: list[str] = []
        self._header_cells: list[str] = []

        self._in_subject_anchor = False
        self._subject_anchor_text: list[str] = []

        self._row_year = ""
        self._row_topic = ""
        self._row_subject = ""
        self._row_href: str | None = None

        self.rows: list[_RowData] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = dict(attrs)

        if tag == "table":
            if not self._in_table:
                self._in_table = True
                self._table_depth = 1
                self._table_matches = False
                self._header_cells = []
                return
            if self._in_table:
                self._table_depth += 1
                return

        if not self._in_table:
            return

        if tag == "tr":
            self._in_tr = True
            self._td_index = 0
            self._current_cell_text = []
            self._in_subject_anchor = False
            self._subject_anchor_text = []

            self._row_year = ""
            self._row_topic = ""
            self._row_subject = ""
            self._row_href = None
            return

        if not self._in_tr:
            return

        if tag == "th":
            self._in_th = True
            self._current_cell_text = []
            return

        if tag == "td":
            self._in_td = True
            self._td_index += 1
            self._current_cell_text = []
            self._in_subject_anchor = False
            self._subject_anchor_text = []
            return

        if tag == "a" and self._in_td and self._td_index == 3:
            href = clean_text(str(attrs_map.get("href") or ""))
            if href and self._row_href is None:
                self._row_href = href
            self._in_subject_anchor = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._in_table:
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_table = False
                self._table_depth = 0
                self._table_matches = False
            return

        if not self._in_table:
            return

        if tag == "a":
            self._in_subject_anchor = False
            return

        if tag == "th" and self._in_th:
            cell = clean_text("".join(self._current_cell_text)).lower()
            if cell:
                self._header_cells.append(cell)
            self._in_th = False
            self._current_cell_text = []
            return

        if tag == "td" and self._in_td:
            cell = clean_text("".join(self._current_cell_text))

            if self._table_matches:
                if self._td_index == 1:
                    self._row_year = cell
                elif self._td_index == 2:
                    self._row_topic = cell
                elif self._td_index == 3:
                    subject_anchor = clean_text("".join(self._subject_anchor_text))
                    self._row_subject = subject_anchor or cell

            self._in_td = False
            self._current_cell_text = []
            self._in_subject_anchor = False
            self._subject_anchor_text = []
            return

        if tag == "tr" and self._in_tr:
            if not self._table_matches and self._header_cells:
                hdr = " ".join(self._header_cells)
                if "year" in hdr and "topic" in hdr and "subject" in hdr:
                    self._table_matches = True

            if self._table_matches:
                year = clean_text(self._row_year)
                topic = clean_text(self._row_topic)
                subject = clean_text(self._row_subject)
                if year and topic and subject and self._row_href:
                    self.rows.append(
                        _RowData(
                            year=year,
                            topic=topic,
                            subject=subject,
                            href=self._row_href,
                        )
                    )

            self._in_tr = False
            self._in_th = False
            self._in_td = False
            self._td_index = 0
            self._current_cell_text = []

            self._in_subject_anchor = False
            self._subject_anchor_text = []

            self._row_year = ""
            self._row_topic = ""
            self._row_subject = ""
            self._row_href = None

    def handle_data(self, data: str) -> None:
        if self._in_th or self._in_td:
            self._current_cell_text.append(data)
        if self._in_td and self._td_index == 3 and self._in_subject_anchor:
            self._subject_anchor_text.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _publish_date_from_year(year_text: str) -> str | None:
    m = re.search(r"\b(\d{4})\b", year_text)
    if not m:
        return None
    return f"{int(m.group(1)):04d}-01-01"


class Crawler:
    name = "info_notes"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))

        request_delay = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.10))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        resp = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        resp.encoding = "utf-8"

        parser = _InfoNotesTableParser()
        parser.feed(resp.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for row in parser.rows:
            if len(out) >= max_total_records:
                break

            href = clean_text(str(row.href or ""))
            if not href:
                continue

            url = _canonicalize(urljoin(page_url, href))
            if not url:
                continue
            if path_ext(url) != ".pdf":
                continue
            if url in seen_urls:
                continue

            out.append(
                ctx.make_record(
                    url=url,
                    name=row.subject,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    publish_date=_publish_date_from_year(row.year),
                    meta={
                        "topic": row.topic,
                        "discovered_from": page_url,
                    },
                )
            )
            seen_urls.add(url)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("topic") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
