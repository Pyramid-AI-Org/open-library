from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
)
from utils.html_links import extract_links


XPPM_MAIN_URL = (
    "https://www.hyd.gov.hk/en/technical_references/technical_document/xppm/index.html"
)
XPPM_CONDITION_URL = (
    "https://www.hyd.gov.hk/en/technical_references/technical_document/xppm/condition/index.html"
)
XPPM_UTLC_URL = (
    "https://www.hyd.gov.hk/en/technical_references/technical_document/xppm/utlc_paper/index.html"
)

_ALLOWED_DOC_EXTS = {".pdf"}
_YEAR_TOKEN_RE = re.compile(r"\b\d+\s*/\s*(\d{2,4})\b")
_FULL_DATE_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")
_EFFECTIVE_FROM_RE = re.compile(
    r"effective\s+from\s+(\d{1,2})\.(\d{1,2})\.(\d{4})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class XppmHit:
    url: str
    name: str
    discovered_from: str
    meta: dict[str, str]


@dataclass(frozen=True)
class _Cell:
    text: str
    links: list[tuple[str, str]]
    rowspan: int


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[_Cell]] = []

        self._row_stack: list[list[_Cell]] = []

        self._in_cell = False
        self._current_cell_text_parts: list[str] = []
        self._current_cell_links: list[tuple[str, str]] = []
        self._current_cell_rowspan = 1

        self._in_a = False
        self._current_a_href: str | None = None
        self._current_a_text_parts: list[str] = []

    def _attrs_to_dict(self, attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t == "tr":
            self._row_stack.append([])
            return

        if not self._row_stack:
            return

        if t in {"td", "th"} and not self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            rowspan_raw = (attrs_map.get("rowspan") or "").strip()
            rowspan = 1
            if rowspan_raw.isdigit():
                rowspan = max(1, int(rowspan_raw))

            self._in_cell = True
            self._current_cell_rowspan = rowspan
            self._current_cell_text_parts = []
            self._current_cell_links = []
            return

        if t == "a" and self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            self._in_a = True
            self._current_a_href = attrs_map.get("href")
            self._current_a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "a" and self._in_a:
            a_text = clean_text("".join(self._current_a_text_parts))
            if self._current_a_href:
                self._current_cell_links.append((self._current_a_href, a_text))
            self._in_a = False
            self._current_a_href = None
            self._current_a_text_parts = []
            return

        if t in {"td", "th"} and self._in_cell:
            if self._row_stack:
                self._row_stack[-1].append(
                    _Cell(
                        text=clean_text("".join(self._current_cell_text_parts)),
                        links=self._current_cell_links,
                        rowspan=self._current_cell_rowspan,
                    )
                )
            self._in_cell = False
            self._current_cell_rowspan = 1
            self._current_cell_text_parts = []
            self._current_cell_links = []
            return

        if t == "tr" and self._row_stack:
            row = self._row_stack.pop()
            if row:
                self.rows.append(row)

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell_text_parts.append(data)
        if self._in_a:
            self._current_a_text_parts.append(data)


def _canonicalize_url(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _make_absolute_pdf_url(*, page_url: str, href: str) -> str | None:
    can = _canonicalize_url(urljoin(page_url, href))
    if not can:
        return None
    if path_ext(can) not in _ALLOWED_DOC_EXTS:
        return None
    return can


def _rows_with_rowspan_spread(rows: list[list[_Cell]]) -> list[list[_Cell]]:
    active: dict[int, tuple[_Cell, int]] = {}
    out: list[list[_Cell]] = []

    for raw_row in rows:
        logical: list[_Cell] = []
        raw_idx = 0
        col = 0

        while raw_idx < len(raw_row) or col in active:
            if col in active:
                span_cell, remaining = active[col]
                logical.append(span_cell)
                if remaining <= 1:
                    del active[col]
                else:
                    active[col] = (span_cell, remaining - 1)
                col += 1
                continue

            if raw_idx < len(raw_row):
                cell = raw_row[raw_idx]
                logical.append(cell)
                if cell.rowspan > 1:
                    active[col] = (cell, cell.rowspan - 1)
                raw_idx += 1
                col += 1
                continue

            col += 1

        out.append(logical)

    return out


def _extract_utlc_publish_date(number_text: str, title_text: str) -> str | None:
    full_date = _FULL_DATE_RE.search(title_text)
    if full_date:
        dd, mm, yyyy = full_date.groups()
        return f"{int(dd):02d}.{int(mm):02d}.{yyyy}"

    m = _YEAR_TOKEN_RE.search(number_text)
    if not m:
        return None

    year_token = m.group(1)
    if len(year_token) == 4:
        return year_token

    yy = int(year_token)
    year = 1900 + yy if yy >= 70 else 2000 + yy
    return str(year)


def _extract_effective_from_date(text: str) -> str | None:
    m = _EFFECTIVE_FROM_RE.search(text)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    return f"{int(dd):02d}.{int(mm):02d}.{yyyy}"


def _fetch_html(
    *,
    session: requests.Session,
    page_url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> str:
    resp = get_with_retries(
        session,
        page_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )
    return resp.text


def parse_xppm_main_hits(html: str) -> list[XppmHit]:
    hits: list[XppmHit] = []
    seen_urls: set[str] = set()

    for link in extract_links(html, base_url=XPPM_MAIN_URL):
        can = _make_absolute_pdf_url(page_url=XPPM_MAIN_URL, href=link.href)
        if not can:
            continue
        if can in seen_urls:
            continue
        seen_urls.add(can)

        hits.append(
            XppmHit(
                url=can,
                name=infer_name_from_link(link.text, can) or can,
                discovered_from=XPPM_MAIN_URL,
                meta={},
            )
        )

    return hits


def parse_xppm_condition_hits(html: str) -> list[XppmHit]:
    parser = _TableParser()
    parser.feed(html)

    rows = _rows_with_rowspan_spread(parser.rows)
    hits: list[XppmHit] = []
    seen_urls: set[str] = set()

    for row in rows:
        if len(row) < 5:
            continue

        form_no = clean_text(row[0].text)
        conditions_of_permits = clean_text(row[1].text)
        download_cell = row[3]
        remark = clean_text(row[4].text)

        if not download_cell.links:
            continue

        for href, link_text in download_cell.links:
            can = _make_absolute_pdf_url(page_url=XPPM_CONDITION_URL, href=href)
            if not can:
                continue
            if can in seen_urls:
                continue
            seen_urls.add(can)

            meta: dict[str, str] = {
                "form_no": form_no,
                "conditions_of_permits": conditions_of_permits,
                "remark": remark,
            }
            effective_from = _extract_effective_from_date(remark)
            if effective_from:
                meta["publish_date"] = effective_from

            hits.append(
                XppmHit(
                    url=can,
                    name=infer_name_from_link(link_text, can) or can,
                    discovered_from=XPPM_CONDITION_URL,
                    meta=meta,
                )
            )

    return hits


def parse_xppm_utlc_hits(html: str) -> list[XppmHit]:
    parser = _TableParser()
    parser.feed(html)

    rows = _rows_with_rowspan_spread(parser.rows)
    hits: list[XppmHit] = []
    seen_urls: set[str] = set()

    for row in rows:
        if len(row) < 2:
            continue

        number_text = clean_text(row[0].text)
        title_cell = row[1]
        if not title_cell.links:
            continue

        for href, link_text in title_cell.links:
            can = _make_absolute_pdf_url(page_url=XPPM_UTLC_URL, href=href)
            if not can:
                continue
            if can in seen_urls:
                continue
            seen_urls.add(can)

            name = infer_name_from_link(link_text, can) or can
            publish_date = _extract_utlc_publish_date(number_text, name)

            meta: dict[str, str] = {}
            if publish_date:
                meta["publish_date"] = publish_date

            hits.append(
                XppmHit(
                    url=can,
                    name=name,
                    discovered_from=XPPM_UTLC_URL,
                    meta=meta,
                )
            )

    return hits


def fetch_and_parse_xppm_hits(
    *,
    session: requests.Session,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> list[XppmHit]:
    main_html = _fetch_html(
        session=session,
        page_url=XPPM_MAIN_URL,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )
    condition_html = _fetch_html(
        session=session,
        page_url=XPPM_CONDITION_URL,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )
    utlc_html = _fetch_html(
        session=session,
        page_url=XPPM_UTLC_URL,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )

    out: list[XppmHit] = []
    out.extend(parse_xppm_main_hits(main_html))
    out.extend(parse_xppm_condition_hits(condition_html))
    out.extend(parse_xppm_utlc_hits(utlc_html))
    return out
