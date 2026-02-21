from __future__ import annotations

import logging
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
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)
from utils.html_links import HtmlLink

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_URL = (
    "https://www.emsd.gov.hk/en/lifts_and_escalators_safety/publications/circulars/index.html"
)


@dataclass(frozen=True)
class _ParsedRow:
    date: str | None
    circular_no: str | None
    category: str | None
    links: list[HtmlLink]


class _CircularTableParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.rows: list[_ParsedRow] = []

        self._in_target_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_td = False
        self._in_th = False

        self._active_spans: dict[int, tuple[int, str]] = {}
        self._row_text_by_col: dict[int, str] = {}
        self._row_links_by_col: dict[int, list[HtmlLink]] = {}
        self._header_by_col: dict[int, str] = {}

        self._col_cursor = 0
        self._current_col = -1
        self._current_rowspan = 1
        self._current_cell_text_parts: list[str] = []
        self._current_cell_links: list[HtmlLink] = []

        self._in_a = False
        self._current_href: str | None = None
        self._current_a_text_parts: list[str] = []

        self._row_has_td = False
        self._row_has_th = False

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()}

    def _next_available_col(self) -> int:
        while self._col_cursor in self._row_text_by_col:
            self._col_cursor += 1
        out = self._col_cursor
        self._col_cursor += 1
        return out

    def _resolve_col_indices(self) -> tuple[int, int, int, int]:
        date_col = 0
        circular_no_col = 1
        circular_links_col = 2
        category_col = 4

        for idx, header in self._header_by_col.items():
            h = clean_text(header).lower()
            if not h:
                continue
            if "date" in h:
                date_col = idx
            elif "circular" in h and "no" in h:
                circular_no_col = idx
            elif h == "circular" or ("circular" in h and "no" not in h):
                circular_links_col = idx
            elif "category" in h:
                category_col = idx

        return date_col, circular_no_col, circular_links_col, category_col

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_target_table and t == "table":
            classes = self._class_set(attrs_map)
            if "color_table" in classes:
                self._in_target_table = True
                self._table_depth = 1
                return

        if not self._in_target_table:
            return

        if t == "table":
            self._table_depth += 1
            return

        if t == "tr":
            self._in_tr = True
            self._in_td = False
            self._in_th = False
            self._row_has_td = False
            self._row_has_th = False

            self._row_text_by_col = {}
            self._row_links_by_col = {}
            self._col_cursor = 0

            # Carry values from active rowspans.
            for col, (remaining, text) in list(self._active_spans.items()):
                self._row_text_by_col[col] = text
                if remaining <= 1:
                    del self._active_spans[col]
                else:
                    self._active_spans[col] = (remaining - 1, text)
            return

        if not self._in_tr:
            return

        if t in {"td", "th"}:
            self._in_td = t == "td"
            self._in_th = t == "th"
            self._row_has_td = self._row_has_td or (t == "td")
            self._row_has_th = self._row_has_th or (t == "th")

            self._current_col = self._next_available_col()
            self._current_rowspan = 1
            try:
                self._current_rowspan = max(1, int(attrs_map.get("rowspan", "1")))
            except ValueError:
                self._current_rowspan = 1

            self._current_cell_text_parts = []
            self._current_cell_links = []
            return

        if t == "a" and (self._in_td or self._in_th):
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_target_table:
            return

        if t == "table":
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_target_table = False
            return

        if self._in_a and t == "a":
            if self._current_href:
                link_text = clean_text("".join(self._current_a_text_parts))
                self._current_cell_links.append(
                    HtmlLink(href=urljoin(self.base_url, self._current_href), text=link_text)
                )
            self._in_a = False
            self._current_href = None
            self._current_a_text_parts = []
            return

        if t in {"td", "th"} and (self._in_td or self._in_th):
            text = clean_text("".join(self._current_cell_text_parts))
            if text:
                self._row_text_by_col[self._current_col] = text

            if self._current_cell_links:
                self._row_links_by_col[self._current_col] = list(self._current_cell_links)

            if self._current_rowspan > 1 and text:
                self._active_spans[self._current_col] = (self._current_rowspan - 1, text)

            if self._in_th and text:
                self._header_by_col[self._current_col] = text

            self._in_td = False
            self._in_th = False
            self._current_col = -1
            self._current_rowspan = 1
            self._current_cell_text_parts = []
            self._current_cell_links = []
            return

        if t == "tr" and self._in_tr:
            self._in_tr = False

            # Skip header rows.
            if self._row_has_th and not self._row_has_td:
                return

            date_col, circular_no_col, circular_links_col, category_col = self._resolve_col_indices()

            links = self._row_links_by_col.get(circular_links_col, [])
            if not links:
                return

            self.rows.append(
                _ParsedRow(
                    date=clean_text(self._row_text_by_col.get(date_col, "")) or None,
                    circular_no=clean_text(self._row_text_by_col.get(circular_no_col, ""))
                    or None,
                    category=clean_text(self._row_text_by_col.get(category_col, "")) or None,
                    links=links,
                )
            )

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._current_a_text_parts.append(data)
        if self._in_td or self._in_th:
            self._current_cell_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "emsd.lifts_and_escalators_circulars"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        try:
            resp = get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
        except Exception as exc:
            logger.error(f"[{self.name}] Failed to fetch {page_url}: {exc}")
            return []

        resp.encoding = "utf-8"

        parser = _CircularTableParser(base_url=page_url)
        parser.feed(resp.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for row in parser.rows:
            for link in row.links:
                can = _canonicalize(link.href)
                if not can:
                    continue
                if path_ext(can) != ".pdf":
                    continue
                if can in seen_urls:
                    # URL-based dedupe only, keep first-seen metadata.
                    continue

                out.append(
                    UrlRecord(
                        url=can,
                        name=clean_text(link.text) or infer_name_from_link(link.text, can),
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={
                            "discovered_from": page_url,
                            "category": row.category,
                            "date": row.date,
                            "circular_no": row.circular_no,
                        },
                    )
                )
                seen_urls.add(can)

                if len(out) >= max_total_records:
                    break
            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(f"[{self.name}] Found {len(out)} circular PDF URLs")
        return out
