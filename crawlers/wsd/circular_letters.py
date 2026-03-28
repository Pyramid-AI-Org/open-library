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


_DEFAULT_PAGE_URL = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/wsd-circular-letters/index.html"
)
_DEFAULT_PDF_SCOPE_PREFIX = "https://www.wsd.gov.hk/"
_CATEGORY_LABELS: dict[str, str] = {
    "1": "Administration",
    "2": "Hong Kong Waterworks Standard Requirements",
    "3": "Application of Water Supply",
    "4": "Approval of Pipes and Fitting",
}


@dataclass(frozen=True)
class _TableCell:
    text: str
    links: list[tuple[str, str]]


@dataclass(frozen=True)
class _TableRow:
    cells: list[_TableCell]


class _MainTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_TableRow] = []

        self._table_depth = 0
        self._in_target_table = False

        self._row_stack: list[list[_TableCell]] = []
        self._in_cell = False
        self._cell_text_parts: list[str] = []
        self._cell_links: list[tuple[str, str]] = []

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

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t == "table":
            attrs_map = self._attrs_to_dict(attrs)
            classes = {
                cls.strip().lower()
                for cls in attrs_map.get("class", "").split()
                if cls.strip()
            }
            if not self._in_target_table and "main_table" in classes:
                self._in_target_table = True
                self._table_depth = 1
                return
            if self._in_target_table:
                self._table_depth += 1
            return

        if not self._in_target_table:
            return

        if t == "tr":
            self._row_stack.append([])
            return

        if t in {"td", "th"} and self._row_stack and not self._in_cell:
            self._in_cell = True
            self._cell_text_parts = []
            self._cell_links = []
            return

        if t == "a" and self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "table" and self._in_target_table:
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_target_table = False
                self._table_depth = 0
            return

        if not self._in_target_table:
            return

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
                    _TableCell(
                        text=clean_text("".join(self._cell_text_parts)),
                        links=self._cell_links,
                    )
                )
            self._in_cell = False
            self._cell_text_parts = []
            self._cell_links = []
            return

        if t == "tr" and self._row_stack:
            row_cells = self._row_stack.pop()
            if row_cells:
                self.rows.append(_TableRow(cells=row_cells))

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "circular_letters"

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
        html = response.text or ""

        parser = _MainTableParser()
        parser.feed(html)

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for row in parser.rows:
            if len(out) >= max_total_records:
                break

            if len(row.cells) < 4:
                continue

            pdf_no = clean_text(row.cells[0].text)
            name = clean_text(row.cells[1].text)
            publish_date_raw = clean_text(row.cells[2].text)
            category_code = clean_text(row.cells[3].text)

            if not pdf_no or not name:
                continue

            # Skip header-like rows from the table body.
            if pdf_no.lower() in {"no."} or name.lower() in {"description"}:
                continue

            description_links = row.cells[1].links
            if not description_links:
                continue

            publish_date = normalize_publish_date(publish_date_raw)
            category_label = _CATEGORY_LABELS.get(category_code)

            chosen_pdf_url: str | None = None
            for href, _link_text in description_links:
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

            meta: dict[str, str] = {
                "discovered_from": page_url,
                "pdf_no": pdf_no,
            }
            if category_label:
                meta["category"] = category_label

            out.append(
                ctx.make_record(
                    url=chosen_pdf_url,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta=meta,
                    publish_date=publish_date,
                )
            )
            seen_urls.add(chosen_pdf_url)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("category") or ""),
                str(r.meta.get("pdf_no") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
