from __future__ import annotations

import random
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)
from utils.html_links import HtmlLink, extract_links, extract_links_in_element
from crawlers.hyd.gis_specifications_helper import (
    GIS_PAGE_URL,
    fetch_and_parse_gis_specification_hits,
)
from crawlers.hyd.xppm_helper import (
    XPPM_MAIN_URL,
    fetch_and_parse_xppm_hits,
)


_ALLOWED_DOC_EXTS = {".pdf"}
_DEFAULT_BASE_URL = "https://www.hyd.gov.hk"
_DEFAULT_START_URL = (
    "https://www.hyd.gov.hk/en/technical_references/technical_document/index.html"
)
_DEFAULT_SCOPE_PREFIX = "/en/technical_references/technical_document/"
_DEFAULT_CONTENT_ELEMENT_ID = "content"
_DEFAULT_EXCLUDE_PAGE_URLS = {
    "https://www.hyd.gov.hk/en/technical_references/technical_document/road_notes/index.html",
    "https://www.hyd.gov.hk/en/technical_references/technical_document/GIS_Specifications/index.html",
    "https://www.hyd.gov.hk/en/technical_references/technical_document/xppm/index.html",
    "https://www.hyd.gov.hk/en/technical_references/technical_document/structures_design_manual_2013/index.html",
}
_GUIDANCE_NOTES_PATH = (
    "/en/technical_references/technical_document/guidance_notes/index.html"
)
_HYD_TECHNICAL_CIRCULARS_PATH = (
    "/en/technical_references/technical_document/hyd_technical_circulars/index.html"
)
_MONTH_YEAR_RE = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
    r")\s+((?:19|20)\d{2})\b",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")


def _canonicalize_url(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


_path_ext = path_ext
_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries


@dataclass(frozen=True)
class _QueueItem:
    url: str
    depth: int


@dataclass(frozen=True)
class _TableLinkRow:
    cells: tuple[str, ...]
    links: tuple[str, ...]
    year_context: str | None


class _TableLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_TableLinkRow] = []

        self._in_heading = False
        self._heading_parts: list[str] = []
        self._current_year: str | None = None

        self._row_stack: list[dict[str, object]] = []

        self._in_cell = False
        self._cell_parts: list[str] = []

        self._in_a = False
        self._a_href: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._heading_parts = []
            return

        if t == "tr":
            self._row_stack.append(
                {
                    "cells": [],
                    "links": [],
                    "year_context": self._current_year,
                }
            )
            return

        if not self._row_stack:
            return

        if t in {"td", "th"} and not self._in_cell:
            self._in_cell = True
            self._cell_parts = []
            return

        if t == "a" and self._in_cell:
            href = None
            for k, v in attrs:
                if k and k.lower() == "href" and v:
                    href = v
                    break
            self._in_a = True
            self._a_href = href
            return

        if t == "br" and self._in_cell:
            self._cell_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t in {"h1", "h2", "h3", "h4", "h5", "h6"} and self._in_heading:
            heading_text = " ".join("".join(self._heading_parts).split())
            year = _extract_year_token(heading_text)
            if year:
                self._current_year = year
            self._in_heading = False
            self._heading_parts = []
            return

        if t == "a" and self._in_a:
            if self._a_href and self._row_stack:
                links = self._row_stack[-1].get("links")
                if isinstance(links, list):
                    links.append(self._a_href)
            self._in_a = False
            self._a_href = None
            return

        if t in {"td", "th"} and self._in_cell:
            if self._row_stack:
                cells = self._row_stack[-1].get("cells")
                if isinstance(cells, list):
                    cells.append(" ".join("".join(self._cell_parts).split()))
            self._in_cell = False
            self._cell_parts = []
            return

        if t == "tr" and self._row_stack:
            row = self._row_stack.pop()
            row_links = row.get("links")
            row_cells = row.get("cells")
            row_year_context = row.get("year_context")
            if isinstance(row_links, list) and row_links:
                self.rows.append(
                    _TableLinkRow(
                        cells=tuple(row_cells) if isinstance(row_cells, list) else (),
                        links=tuple(row_links),
                        year_context=(
                            row_year_context
                            if isinstance(row_year_context, str)
                            else None
                        ),
                    )
                )
            self._in_cell = False
            self._cell_parts = []
            self._in_a = False
            self._a_href = None

    def handle_data(self, data: str) -> None:
        if self._in_heading:
            self._heading_parts.append(data)
        if self._in_cell:
            self._cell_parts.append(data)


def _extract_year_token(text: str) -> str | None:
    m = _YEAR_RE.search(text)
    if not m:
        return None
    return m.group(1)


def _extract_month_year_token(text: str) -> str | None:
    m = _MONTH_YEAR_RE.search(text)
    if not m:
        return None
    month_token = m.group(1)
    year_token = m.group(2)
    month = month_token[:1].upper() + month_token[1:].lower()
    return f"{month} {year_token}"


def _build_guidance_notes_date_map(html: str, *, page_url: str) -> dict[str, str]:
    parser = _TableLinkParser()
    parser.feed(html)

    date_map: dict[str, str] = {}
    for row in parser.rows:
        if not row.cells:
            continue
        row_date = _extract_month_year_token(row.cells[0])
        if not row_date:
            continue

        for href in row.links:
            can = _canonicalize_url(urljoin(page_url, href))
            if not can or _path_ext(can) not in _ALLOWED_DOC_EXTS:
                continue
            if can not in date_map:
                date_map[can] = row_date

    return date_map


def _build_hyd_technical_circulars_date_map(
    html: str, *, page_url: str
) -> dict[str, str]:
    parser = _TableLinkParser()
    parser.feed(html)

    date_map: dict[str, str] = {}
    for row in parser.rows:
        row_text = " ".join(row.cells)
        row_date = _extract_month_year_token(row_text)
        if not row_date and row.cells:
            row_date = _extract_year_token(row.cells[0])
        if not row_date:
            row_date = row.year_context
        if not row_date:
            continue

        for href in row.links:
            can = _canonicalize_url(urljoin(page_url, href))
            if not can or _path_ext(can) not in _ALLOWED_DOC_EXTS:
                continue
            if can not in date_map:
                date_map[can] = row_date

    return date_map


def _iter_links(
    html: str, *, base_url: str, content_element_id: str
) -> Iterable[HtmlLink]:
    scoped = extract_links_in_element(
        html,
        base_url=base_url,
        element_id=content_element_id,
    )
    if scoped:
        return scoped
    return extract_links(html, base_url)


class Crawler:
    name = "hyd_technical_documents"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        base_url = str(cfg.get("base_url", _DEFAULT_BASE_URL)).strip().rstrip("/")
        start_url_raw = str(cfg.get("start_url", _DEFAULT_START_URL)).strip()
        scope_prefix = str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip()
        if not scope_prefix.startswith("/"):
            scope_prefix = "/" + scope_prefix

        content_element_id = (
            str(cfg.get("content_element_id", _DEFAULT_CONTENT_ELEMENT_ID)).strip()
            or _DEFAULT_CONTENT_ELEMENT_ID
        )

        exclude_urls_cfg = cfg.get("exclude_page_urls", [])
        exclude_urls: set[str] = set()
        if isinstance(exclude_urls_cfg, list):
            for raw in exclude_urls_cfg:
                if not isinstance(raw, str):
                    continue
                can = _canonicalize_url(raw)
                if can:
                    exclude_urls.add(can)
        if not exclude_urls:
            exclude_urls = {
                can
                for can in (
                    _canonicalize_url(url) for url in _DEFAULT_EXCLUDE_PAGE_URLS
                )
                if can
            }

        max_depth = int(cfg.get("max_depth", 4))
        max_pages = int(cfg.get("max_pages", 500))
        max_out_links_per_page = int(cfg.get("max_out_links_per_page", 800))
        max_total_records = int(cfg.get("max_total_records", 50000))

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        base_can = _canonicalize_url(base_url)
        start_url = _canonicalize_url(start_url_raw)
        if not base_can or not start_url:
            return []

        base_netloc = urlparse(base_can).netloc.lower()

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        queue: list[_QueueItem] = [_QueueItem(url=start_url, depth=0)]
        visited_pages: set[str] = set()
        queued_pages: set[str] = {start_url}
        seen_docs: set[str] = set()
        out: list[UrlRecord] = []

        while queue:
            item = queue.pop(0)
            queued_pages.discard(item.url)

            if item.url in visited_pages:
                continue
            if item.url in exclude_urls:
                continue
            if len(visited_pages) >= max_pages:
                break

            page_parsed = urlparse(item.url)
            if page_parsed.netloc.lower() != base_netloc:
                continue
            if not page_parsed.path.startswith(scope_prefix):
                continue

            visited_pages.add(item.url)

            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            if ctx.debug:
                print(f"[{self.name}] Fetch(depth={item.depth}) -> {item.url}")

            try:
                resp = _get_with_retries(
                    session,
                    item.url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except Exception as exc:
                if ctx.debug:
                    print(f"[{self.name}] Skip page fetch failure: {item.url} ({exc})")
                continue

            page_date_map: dict[str, str] = {}
            if page_parsed.path == _GUIDANCE_NOTES_PATH:
                page_date_map = _build_guidance_notes_date_map(
                    resp.text,
                    page_url=item.url,
                )
            elif page_parsed.path == _HYD_TECHNICAL_CIRCULARS_PATH:
                page_date_map = _build_hyd_technical_circulars_date_map(
                    resp.text,
                    page_url=item.url,
                )

            link_count = 0
            for link in _iter_links(
                resp.text,
                base_url=item.url,
                content_element_id=content_element_id,
            ):
                if link_count >= max_out_links_per_page:
                    break
                link_count += 1

                candidate = _canonicalize_url(link.href)
                if not candidate:
                    continue

                ext = _path_ext(candidate)
                if ext in _ALLOWED_DOC_EXTS:
                    doc_parsed = urlparse(candidate)
                    if doc_parsed.netloc.lower() != base_netloc:
                        continue
                    if candidate in seen_docs:
                        continue

                    seen_docs.add(candidate)
                    meta: dict[str, str] = {"discovered_from": item.url}
                    publish_date = page_date_map.get(candidate)
                    if publish_date:
                        meta["publish_date"] = publish_date
                    out.append(
                        ctx.make_record(
                            url=candidate,
                            name=infer_name_from_link(link.text, candidate),
                            discovered_at_utc=ctx.run_date_utc,
                            source=self.name,
                            meta=meta,
                        )
                    )
                    if len(out) >= max_total_records:
                        break
                    continue

                if item.depth >= max_depth:
                    continue

                page_next = urlparse(candidate)
                if page_next.netloc.lower() != base_netloc:
                    continue
                if not page_next.path.startswith(scope_prefix):
                    continue
                if candidate in exclude_urls:
                    continue
                if candidate in visited_pages or candidate in queued_pages:
                    continue

                queue.append(_QueueItem(url=candidate, depth=item.depth + 1))
                queued_pages.add(candidate)

            if len(out) >= max_total_records:
                break

        try:
            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            gis_hits = fetch_and_parse_gis_specification_hits(
                session=session,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )
        except Exception as exc:
            if ctx.debug:
                print(f"[{self.name}] Skip page fetch failure: {GIS_PAGE_URL} ({exc})")
            gis_hits = []

        for hit in gis_hits:
            if hit.url in seen_docs:
                continue

            seen_docs.add(hit.url)
            meta: dict[str, str] = {"discovered_from": GIS_PAGE_URL}
            if hit.publish_date:
                meta["publish_date"] = hit.publish_date

            out.append(
                ctx.make_record(
                    url=hit.url,
                    name=hit.name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta=meta,
                )
            )
            if len(out) >= max_total_records:
                break

        if len(out) < max_total_records:
            try:
                if request_delay_seconds > 0:
                    _sleep_seconds(
                        request_delay_seconds
                        + random.uniform(0.0, request_jitter_seconds)
                    )

                xppm_hits = fetch_and_parse_xppm_hits(
                    session=session,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except Exception as exc:
                if ctx.debug:
                    print(
                        f"[{self.name}] Skip page fetch failure: {XPPM_MAIN_URL} ({exc})"
                    )
                xppm_hits = []

            for hit in xppm_hits:
                if hit.url in seen_docs:
                    continue

                seen_docs.add(hit.url)
                meta: dict[str, str] = {"discovered_from": hit.discovered_from}
                if hit.meta:
                    meta.update(hit.meta)

                out.append(
                    ctx.make_record(
                        url=hit.url,
                        name=hit.name,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta=meta,
                    )
                )
                if len(out) >= max_total_records:
                    break

        out.sort(
            key=lambda r: (
                r.url,
                r.name or "",
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
