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
    normalize_publish_date,
    path_ext,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element


_DEFAULT_PAGE_URL = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/"
    "mechanical-electrical-standard-specification/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/"
    "mechanical-electrical-standard-specification/"
)
_MONTH_YEAR_RE = re.compile(r"^(\d{1,2})\.(\d{4})$")


@dataclass(frozen=True)
class _Cell:
    text: str
    links: list[tuple[str, str]]
    classes: set[str]
    rowspan: int
    colspan: int


@dataclass(frozen=True)
class _TableRow:
    classes: set[str]
    cells: list[_Cell]


@dataclass(frozen=True)
class _CandidateRecord:
    url: str
    name: str | None
    discovered_from: str
    section_name: str
    pdf_no: str
    publish_date: str | None


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_TableRow] = []

        self._row_stack: list[list[_Cell]] = []
        self._row_classes_stack: list[set[str]] = []

        self._in_cell = False
        self._cell_text_parts: list[str] = []
        self._cell_links: list[tuple[str, str]] = []
        self._cell_classes: set[str] = set()
        self._cell_rowspan = 1
        self._cell_colspan = 1

        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

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

    @staticmethod
    def _parse_positive_int(raw: str | None) -> int:
        text = (raw or "").strip()
        if not text.isdigit():
            return 1
        return max(1, int(text))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t == "tr":
            attrs_map = self._attrs_to_dict(attrs)
            self._row_stack.append([])
            self._row_classes_stack.append(self._class_set(attrs_map))
            return

        if not self._row_stack:
            return

        if t in {"td", "th"} and not self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            self._in_cell = True
            self._cell_text_parts = []
            self._cell_links = []
            self._cell_classes = self._class_set(attrs_map)
            self._cell_rowspan = self._parse_positive_int(attrs_map.get("rowspan"))
            self._cell_colspan = self._parse_positive_int(attrs_map.get("colspan"))
            return

        if t == "a" and self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "a" and self._in_a:
            link_text = clean_text("".join(self._a_text_parts))
            if self._a_href:
                self._cell_links.append((self._a_href, link_text))
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            return

        if t in {"td", "th"} and self._in_cell:
            if self._row_stack:
                self._row_stack[-1].append(
                    _Cell(
                        text=clean_text("".join(self._cell_text_parts)),
                        links=self._cell_links,
                        classes=self._cell_classes,
                        rowspan=self._cell_rowspan,
                        colspan=self._cell_colspan,
                    )
                )
            self._in_cell = False
            self._cell_text_parts = []
            self._cell_links = []
            self._cell_classes = set()
            self._cell_rowspan = 1
            self._cell_colspan = 1
            return

        if t == "tr" and self._row_stack:
            row = self._row_stack.pop()
            row_classes = self._row_classes_stack.pop() if self._row_classes_stack else set()
            if row:
                self.rows.append(_TableRow(classes=row_classes, cells=row))

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _spread_rowspans(rows: list[_TableRow]) -> list[_TableRow]:
    active: dict[int, tuple[_Cell, int]] = {}
    out: list[_TableRow] = []

    for raw_row in rows:
        logical: list[_Cell] = []
        raw_idx = 0
        col = 0
        raw_cells = raw_row.cells

        while raw_idx < len(raw_cells) or (active and col <= max(active)):
            if col in active:
                span_cell, remaining = active[col]
                logical.append(span_cell)
                if remaining <= 1:
                    del active[col]
                else:
                    active[col] = (span_cell, remaining - 1)
                col += 1
                continue

            if raw_idx >= len(raw_cells):
                col += 1
                continue

            cell = raw_cells[raw_idx]
            raw_idx += 1

            span_cols = max(1, cell.colspan)
            for offset in range(span_cols):
                logical.append(cell)
                if cell.rowspan > 1:
                    active[col + offset] = (cell, cell.rowspan - 1)
            col += span_cols

        out.append(_TableRow(classes=raw_row.classes, cells=logical))

    return out


def _parse_publish_date_from_revision(value: str) -> str | None:
    text = clean_text(value)
    if not text:
        return None

    m = _MONTH_YEAR_RE.fullmatch(text)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}-01"

    return normalize_publish_date(text)


class Crawler:
    name = "mechanical_electrical_ss"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        scope_prefix = str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip().rstrip("/") + "/"
        content_element_id = str(cfg.get("content_element_id", "content")).strip() or "content"
        max_subpages = int(cfg.get("max_subpages", 20))
        max_total_records = int(cfg.get("max_total_records", 50000))

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

        def _fetch(url: str) -> str:
            if request_delay > 0:
                sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))
            resp = get_with_retries(
                session,
                url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            resp.encoding = "utf-8"
            return resp.text or ""

        landing_html = _fetch(page_url)

        links = extract_links_in_element(
            landing_html,
            base_url=page_url,
            element_id=content_element_id,
        )
        if not links:
            links = extract_links(landing_html, base_url=page_url)

        subpages: list[str] = []
        seen_subpages: set[str] = set()

        for link in links:
            can = _canonicalize(link.href)
            if not can:
                continue
            if not can.startswith(scope_prefix):
                continue
            if can == page_url:
                continue
            if not can.endswith("/index.html"):
                continue
            if "/volume-" not in can:
                continue
            if can in seen_subpages:
                continue
            seen_subpages.add(can)
            subpages.append(can)

        subpages.sort()
        if len(subpages) > max_subpages:
            subpages = subpages[:max_subpages]

        out: list[UrlRecord] = []
        latest_by_doc_key: dict[tuple[str, str, str, str], _CandidateRecord] = {}

        def _rank_publish_date(value: str | None) -> tuple[int, str]:
            if value:
                return (1, value)
            return (0, "")

        for subpage_url in subpages:
            if len(latest_by_doc_key) >= max_total_records:
                break

            html = _fetch(subpage_url)
            parser = _TableParser()
            parser.feed(html)

            rows = _spread_rowspans(parser.rows)
            current_section = ""
            in_first_section = False

            for row in rows:
                if len(latest_by_doc_key) >= max_total_records:
                    break
                if not row.cells:
                    continue

                first_cell = row.cells[0]
                if "sub_hd" in first_cell.classes:
                    current_section = clean_text(first_cell.text)
                    in_first_section = True
                    continue

                # Ignore preface rows before the first section heading row.
                if not in_first_section:
                    continue

                if len(row.cells) < 4:
                    continue

                pdf_no = clean_text(row.cells[0].text)
                title = clean_text(row.cells[1].text)

                if not title or title.lower() == "title":
                    continue

                revision_cell = row.cells[2]
                revision_text = clean_text(revision_cell.text)
                publish_date = _parse_publish_date_from_revision(revision_text)

                for href, _link_text in revision_cell.links:
                    pdf_url = _canonicalize(urljoin(subpage_url, href))
                    if not pdf_url:
                        continue
                    if path_ext(pdf_url) != ".pdf":
                        continue

                    doc_key = (subpage_url, current_section, pdf_no, title)
                    candidate = _CandidateRecord(
                        url=pdf_url,
                        name=title or None,
                        discovered_from=subpage_url,
                        section_name=current_section,
                        pdf_no=pdf_no,
                        publish_date=publish_date,
                    )

                    current = latest_by_doc_key.get(doc_key)
                    if current is None or _rank_publish_date(candidate.publish_date) > _rank_publish_date(
                        current.publish_date
                    ):
                        latest_by_doc_key[doc_key] = candidate

        seen_urls: set[str] = set()
        for _, candidate in sorted(latest_by_doc_key.items(), key=lambda item: item[0]):
            if candidate.url in seen_urls:
                continue
            out.append(
                ctx.make_record(
                    url=candidate.url,
                    name=candidate.name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "discovered_from": candidate.discovered_from,
                        "section_name": candidate.section_name,
                        "pdf_no": candidate.pdf_no,
                    },
                    publish_date=candidate.publish_date,
                )
            )
            seen_urls.add(candidate.url)
            if len(out) >= max_total_records:
                break

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("section_name") or ""),
                str(r.meta.get("pdf_no") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
