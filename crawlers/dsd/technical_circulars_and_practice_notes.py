from __future__ import annotations

import base64
import json
import random
import re
import zlib
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
    "https://www.dsd.gov.hk/EN/Technical_Documents/DSD_Technical_Circulars_and_Practice_Notes/index.html"
)
_DEFAULT_CIRCULARS_PAGE_URL = (
    "https://www.dsd.gov.hk/EN/Technical_Documents/DSD_Technical_Circulars_and_Practice_Notes/"
    "DSD_Technical_Circulars/index.html"
)
_DEFAULT_PRACTICE_NOTES_PAGE_URL = (
    "https://www.dsd.gov.hk/EN/Technical_Documents/DSD_Technical_Circulars_and_Practice_Notes/"
    "DSD_Practice_Notes/index.html"
)
_DEFAULT_CIRCULARS_DATA_JS_URL = (
    "https://www.dsd.gov.hk/assets/js/EN/guideline-data-compress-165.js"
)
_DEFAULT_PRACTICE_NOTES_DATA_JS_URL = (
    "https://www.dsd.gov.hk/assets/js/EN/guideline-data-compress-166.js"
)

_GUIDELINE_BLOB_RE = re.compile(r"guidelineData\s*=\s*'([^']+)'", re.IGNORECASE)
_MONTH_YEAR_RE = re.compile(r"^\s*(\d{1,2})\s*/\s*(\d{2}|\d{4})\s*$")


@dataclass(frozen=True)
class _RowData:
    name: str
    href: str
    publish_date: str | None
    discovered_from: str


class _TopPagePdfParser(HTMLParser):
    """Extract subject + PDF URL rows from the top-level table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_tr = False
        self._in_td = False
        self._in_link = False
        self._current_td_text: list[str] = []
        self._current_tds: list[str] = []
        self._current_href = ""
        self._current_row_link = ""
        self.rows: list[_RowData] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._in_tr = True
            self._current_tds = []
            self._current_row_link = ""
            return

        if not self._in_tr:
            return

        if tag == "td":
            self._in_td = True
            self._current_td_text = []
            return

        if tag == "a":
            self._in_link = True
            attrs_map = dict(attrs)
            self._current_href = clean_text(str(attrs_map.get("href") or ""))

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._in_link = False
            return

        if tag == "td" and self._in_td:
            td_text = clean_text("".join(self._current_td_text))
            self._current_tds.append(td_text)
            self._in_td = False
            self._current_td_text = []
            return

        if tag == "tr" and self._in_tr:
            if len(self._current_tds) >= 2 and self._current_row_link:
                subject = self._current_tds[0]
                self.rows.append(
                    _RowData(
                        name=subject,
                        href=self._current_row_link,
                        publish_date=None,
                        discovered_from="",
                    )
                )

            self._in_tr = False
            self._current_tds = []
            self._current_row_link = ""

    def handle_data(self, data: str) -> None:
        if self._in_td:
            self._current_td_text.append(data)

        if self._in_link and self._current_href and path_ext(self._current_href) == ".pdf":
            self._current_row_link = self._current_href


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _parse_item_no_publish_date(item_no: str) -> str | None:
    text = clean_text(item_no)
    if not text:
        return None

    match = _MONTH_YEAR_RE.match(text)
    if not match:
        return None

    month = int(match.group(1))
    year_text = match.group(2)
    if not (1 <= month <= 12):
        return None

    year = int(year_text)
    if len(year_text) == 2:
        year += 1900

    return normalize_publish_date(f"{year:04d}-{month:02d}-01")


def _extract_rows_from_guideline_data(js_text: str, page_url: str) -> list[_RowData]:
    blob_match = _GUIDELINE_BLOB_RE.search(js_text)
    if not blob_match:
        return []

    try:
        compressed = base64.b64decode(blob_match.group(1))
        decompressed = zlib.decompress(compressed, -zlib.MAX_WBITS)
        items = json.loads(decompressed.decode("utf-8"))
    except (ValueError, zlib.error, json.JSONDecodeError):
        return []

    out: list[_RowData] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        title = clean_text(str(item.get("title") or ""))
        href = clean_text(str(item.get("link") or ""))
        if not title or not href:
            continue

        publish_date = _parse_item_no_publish_date(str(item.get("item_no") or ""))
        out.append(
            _RowData(
                name=title,
                href=href,
                publish_date=publish_date,
                discovered_from=page_url,
            )
        )

    return out


def _extract_rows_from_top_page(html_text: str, page_url: str) -> list[_RowData]:
    parser = _TopPagePdfParser()
    parser.feed(html_text)

    out: list[_RowData] = []
    for row in parser.rows:
        if not row.name or not row.href:
            continue
        out.append(
            _RowData(
                name=row.name,
                href=row.href,
                publish_date=None,
                discovered_from=page_url,
            )
        )
    return out


class Crawler:
    name = "technical_circulars_and_practice_notes"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))
        technical_circulars_page_url = clean_text(
            str(cfg.get("technical_circulars_page_url") or _DEFAULT_CIRCULARS_PAGE_URL)
        )
        practice_notes_page_url = clean_text(
            str(cfg.get("practice_notes_page_url") or _DEFAULT_PRACTICE_NOTES_PAGE_URL)
        )
        technical_circulars_data_js_url = clean_text(
            str(
                cfg.get("technical_circulars_data_js_url")
                or _DEFAULT_CIRCULARS_DATA_JS_URL
            )
        )
        practice_notes_data_js_url = clean_text(
            str(cfg.get("practice_notes_data_js_url") or _DEFAULT_PRACTICE_NOTES_DATA_JS_URL)
        )

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

        all_rows: list[_RowData] = []

        if page_url:
            page_response = get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            page_response.encoding = "utf-8"
            all_rows.extend(_extract_rows_from_top_page(page_response.text or "", page_url))

        for target_page_url, target_data_js_url in (
            (technical_circulars_page_url, technical_circulars_data_js_url),
            (practice_notes_page_url, practice_notes_data_js_url),
        ):
            if not target_page_url or not target_data_js_url:
                continue

            js_response = get_with_retries(
                session,
                target_data_js_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            js_response.encoding = "utf-8"

            all_rows.extend(
                _extract_rows_from_guideline_data(js_response.text or "", target_page_url)
            )

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()
        for row in all_rows:
            if len(out) >= max_total_records:
                break

            candidate_url = _canonicalize(urljoin(row.discovered_from, row.href))
            if not candidate_url:
                continue
            if path_ext(candidate_url) != ".pdf":
                continue
            if candidate_url in seen_urls:
                continue

            out.append(
                ctx.make_record(
                    url=candidate_url,
                    name=row.name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": row.discovered_from},
                    publish_date=row.publish_date,
                )
            )
            seen_urls.add(candidate_url)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out