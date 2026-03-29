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
    path_ext,
    sleep_seconds,
)


_DEFAULT_PAGE_URL = "https://www.hkfsd.gov.hk/eng/fire_protection/notices/"
_DEFAULT_PDF_SCOPE_PREFIX = "https://www.hkfsd.gov.hk/"


@dataclass(frozen=True)
class _TableCell:
    text: str
    links: list[str]


@dataclass(frozen=True)
class _TableRow:
    cells: list[_TableCell]
    heading_name: str


class _NoticesRowsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_TableRow] = []

        self._in_table = False
        self._table_depth = 0
        self._current_heading = ""

        self._in_h2 = False
        self._h2_text_parts: list[str] = []

        self._in_row = False
        self._row_depth = 0
        self._row_cells: list[_TableCell] = []

        self._in_col = False
        self._col_depth = 0
        self._col_text_parts: list[str] = []
        self._col_links: list[str] = []

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

    @staticmethod
    def _is_col_div(classes: set[str]) -> bool:
        if "col" in classes:
            return True
        return any(cls.startswith("col-") for cls in classes)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            classes = self._classes(attrs_map.get("class", ""))

            if self._in_table:
                self._table_depth += 1

                if self._in_row:
                    self._row_depth += 1
                    if self._is_col_div(classes) and not self._in_col:
                        self._in_col = True
                        self._col_depth = 1
                        self._col_text_parts = []
                        self._col_links = []
                    elif self._in_col:
                        self._col_depth += 1
                    return

                if "row" in classes and "title" not in classes:
                    self._in_row = True
                    self._row_depth = 1
                    self._row_cells = []
                return

            if "table" in classes:
                self._in_table = True
                self._table_depth = 1
                self._current_heading = ""
            return

        if self._in_table and t == "h2":
            self._in_h2 = True
            self._h2_text_parts = []
            return

        if self._in_col and t == "a":
            href = clean_text(str(attrs_map.get("href") or ""))
            if href:
                self._col_links.append(href)

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "h2" and self._in_h2:
            self._current_heading = clean_text("".join(self._h2_text_parts))
            self._in_h2 = False
            self._h2_text_parts = []
            return

        if t != "div":
            return

        if self._in_row:
            if self._in_col:
                self._col_depth -= 1
                if self._col_depth <= 0:
                    self._row_cells.append(
                        _TableCell(
                            text=clean_text("".join(self._col_text_parts)),
                            links=self._col_links,
                        )
                    )
                    self._in_col = False
                    self._col_depth = 0
                    self._col_text_parts = []
                    self._col_links = []

            self._row_depth -= 1
            if self._row_depth <= 0:
                if self._row_cells:
                    self.rows.append(
                        _TableRow(
                            cells=self._row_cells,
                            heading_name=self._current_heading,
                        )
                    )
                self._in_row = False
                self._row_depth = 0
                self._row_cells = []

        if self._in_table:
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_table = False
                self._table_depth = 0
                self._current_heading = ""

    def handle_data(self, data: str) -> None:
        if self._in_h2:
            self._h2_text_parts.append(data)
        if self._in_col:
            self._col_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "notices"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        pdf_scope_prefix = str(cfg.get("pdf_scope_prefix", _DEFAULT_PDF_SCOPE_PREFIX)).strip()

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
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

        response = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"

        parser = _NoticesRowsParser()
        parser.feed(response.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for row in parser.rows:
            if len(out) >= max_total_records:
                break
            if len(row.cells) < 2:
                continue

            name = clean_text(row.cells[0].text)
            if not name or name.lower() == "item":
                continue

            heading_name = clean_text(row.heading_name)

            link_candidates: list[str] = []
            link_candidates.extend(row.cells[1].links)
            if not link_candidates:
                for cell in row.cells:
                    link_candidates.extend(cell.links)

            chosen_pdf_url: str | None = None
            for href in link_candidates:
                candidate = _canonicalize(urljoin(page_url, href))
                if not candidate:
                    continue
                if path_ext(candidate) != ".pdf":
                    continue
                if pdf_scope_prefix and not candidate.startswith(pdf_scope_prefix):
                    continue
                chosen_pdf_url = candidate
                break

            if not chosen_pdf_url or chosen_pdf_url in seen_urls:
                continue

            out.append(
                ctx.make_record(
                    url=chosen_pdf_url,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "discovered_from": page_url,
                        "heading_name": heading_name,
                    },
                    publish_date=None,
                )
            )
            seen_urls.add(chosen_pdf_url)

        out.sort(
            key=lambda r: (
                str(r.meta.get("heading_name") or ""),
                r.url,
                str(r.name or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
