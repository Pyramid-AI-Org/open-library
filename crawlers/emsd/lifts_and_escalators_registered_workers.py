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
from utils.html_links import extract_links, extract_links_in_element

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_URL = (
    "https://www.emsd.gov.hk/tc/lifts_and_escalators_safety/"
    "information_for_the_registered_workers/index.html"
)


@dataclass(frozen=True)
class _TableRow:
    date: str | None
    links: list[tuple[str, str]]


class _PlainTableParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.rows: list[_TableRow] = []

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_td = False
        self._in_th = False
        self._in_a = False

        self._current_col = -1
        self._current_rowspan = 1
        self._active_spans: dict[int, tuple[int, str]] = {}
        self._row_text_by_col: dict[int, str] = {}
        self._row_links_by_col: dict[int, list[tuple[str, str]]] = {}
        self._col_cursor = 0

        self._cell_text_parts: list[str] = []
        self._cell_links: list[tuple[str, str]] = []

        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

        self._row_has_td = False

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

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_table and t == "table":
            classes = self._class_set(attrs_map)
            if "plain_table" in classes:
                self._in_table = True
                self._table_depth = 1
                return

        if not self._in_table:
            return

        if t == "table":
            self._table_depth += 1
            return

        if t == "tr":
            self._in_tr = True
            self._row_has_td = False
            self._row_text_by_col = {}
            self._row_links_by_col = {}
            self._col_cursor = 0

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

            self._current_col = self._next_available_col()
            try:
                self._current_rowspan = max(1, int(attrs_map.get("rowspan", "1")))
            except ValueError:
                self._current_rowspan = 1

            self._cell_text_parts = []
            self._cell_links = []
            return

        if t == "a" and (self._in_td or self._in_th):
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_table:
            return

        if t == "table":
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_table = False
            return

        if t == "a" and self._in_a:
            if self._a_href:
                self._cell_links.append(
                    (
                        urljoin(self.base_url, self._a_href),
                        clean_text("".join(self._a_text_parts)),
                    )
                )
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            return

        if t in {"td", "th"} and (self._in_td or self._in_th):
            text = clean_text("".join(self._cell_text_parts))
            if text:
                self._row_text_by_col[self._current_col] = text
            if self._cell_links:
                self._row_links_by_col[self._current_col] = list(self._cell_links)
            if self._current_rowspan > 1 and text:
                self._active_spans[self._current_col] = (self._current_rowspan - 1, text)

            self._in_td = False
            self._in_th = False
            self._current_col = -1
            self._current_rowspan = 1
            self._cell_text_parts = []
            self._cell_links = []
            return

        if t == "tr" and self._in_tr:
            self._in_tr = False
            if not self._row_has_td:
                return

            links = self._row_links_by_col.get(1, [])
            if not links:
                return

            date_value = clean_text(self._row_text_by_col.get(0, "")) or None
            self.rows.append(_TableRow(date=date_value, links=links))

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._a_text_parts.append(data)
        if self._in_td or self._in_th:
            self._cell_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "emsd.lifts_and_escalators_registered_workers"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        content_element_id = str(cfg.get("content_element_id", "content")).strip()

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
        html = resp.text or ""

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        # 1) Table PDFs with date metadata.
        table_parser = _PlainTableParser(base_url=page_url)
        table_parser.feed(html)

        for row in table_parser.rows:
            for href, link_text in row.links:
                can = _canonicalize(href)
                if not can or path_ext(can) != ".pdf":
                    continue
                if can in seen_urls:
                    continue

                out.append(
                    UrlRecord(
                        url=can,
                        name=clean_text(link_text) or infer_name_from_link(link_text, can),
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={
                            "date": row.date,
                            "discovered_from": page_url,
                        },
                    )
                )
                seen_urls.add(can)

                if len(out) >= max_total_records:
                    break
            if len(out) >= max_total_records:
                break

        if len(out) < max_total_records:
            # 2) Free-form PDFs on the page (without date).
            links = extract_links_in_element(
                html,
                base_url=page_url,
                element_id=content_element_id,
            )
            if not links:
                links = extract_links(html, base_url=page_url)

            for link in links:
                can = _canonicalize(link.href)
                if not can or path_ext(can) != ".pdf":
                    continue
                if can in seen_urls:
                    continue

                out.append(
                    UrlRecord(
                        url=can,
                        name=clean_text(link.text) or infer_name_from_link(link.text, can),
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={"discovered_from": page_url},
                    )
                )
                seen_urls.add(can)

                if len(out) >= max_total_records:
                    break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(f"[{self.name}] Found {len(out)} PDF URLs")
        return out
