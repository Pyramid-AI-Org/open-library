from __future__ import annotations

import base64
import json
import random
import re
import zlib
from dataclasses import dataclass
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
    "https://www.dsd.gov.hk/EN/Technical_Documents/Technical_Papers_Published_by_DSD/index.html"
)
_DEFAULT_DATA_JS_URL = (
    "https://www.dsd.gov.hk/assets/js/EN/technicalpaper-data-compress.js"
)
_DEFAULT_DIVISION_DATA_JS_URL = (
    "https://www.dsd.gov.hk/assets/js/EN/technicalpaperdivision-data-compress.js"
)

_TECHNICAL_PAPER_BLOB_RE = re.compile(
    r"technicalPaperData\s*=\s*'([^']+)'", re.IGNORECASE
)
_TECHNICAL_PAPER_DIVISION_BLOB_RE = re.compile(
    r"technicalPaperDivisionData\s*=\s*'([^']+)'", re.IGNORECASE
)


@dataclass(frozen=True)
class _RowData:
    name: str
    href: str
    publish_date: str | None
    division: str


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_blob_items(js_text: str, blob_re: re.Pattern[str]) -> list[dict[str, object]]:
    blob_match = blob_re.search(js_text)
    if not blob_match:
        return []

    try:
        compressed = base64.b64decode(blob_match.group(1))
        decompressed = zlib.decompress(compressed, -zlib.MAX_WBITS)
        items = json.loads(decompressed.decode("utf-8"))
    except (ValueError, zlib.error, json.JSONDecodeError):
        return []

    out: list[dict[str, object]] = []
    for item in items:
        if isinstance(item, dict):
            out.append(item)
    return out


def _extract_division_map(js_text: str) -> dict[str, str]:
    items = _extract_blob_items(js_text, _TECHNICAL_PAPER_DIVISION_BLOB_RE)
    out: dict[str, str] = {}
    for item in items:
        division_id = clean_text(str(item.get("id") or ""))
        title = clean_text(str(item.get("title") or ""))
        if not division_id or not title:
            continue
        out[division_id] = title
    return out


def _extract_year_publish_date(year_value: object) -> str | None:
    year_text = clean_text(str(year_value or ""))
    if not re.fullmatch(r"\d{4}", year_text):
        return None
    return normalize_publish_date(f"{int(year_text):04d}-01-01")


def _extract_rows_from_technical_paper_data(
    js_text: str,
    division_map: dict[str, str],
) -> list[_RowData]:
    items = _extract_blob_items(js_text, _TECHNICAL_PAPER_BLOB_RE)
    out: list[_RowData] = []
    for item in items:
        name = clean_text(str(item.get("title") or ""))
        href = clean_text(str(item.get("link") or ""))
        if not name or not href:
            continue

        division_title = clean_text(str(item.get("division_title") or ""))
        division_id = clean_text(str(item.get("division") or ""))
        division = division_title or division_map.get(division_id, "")

        out.append(
            _RowData(
                name=name,
                href=href,
                publish_date=_extract_year_publish_date(item.get("year")),
                division=division,
            )
        )
    return out


class Crawler:
    name = "technical_papers_published_by_dsd"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))
        data_js_url = clean_text(str(cfg.get("data_js_url") or _DEFAULT_DATA_JS_URL))
        division_data_js_url = clean_text(
            str(cfg.get("division_data_js_url") or _DEFAULT_DIVISION_DATA_JS_URL)
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

        division_map: dict[str, str] = {}
        if division_data_js_url:
            division_response = get_with_retries(
                session,
                division_data_js_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            division_response.encoding = "utf-8"
            division_map = _extract_division_map(division_response.text or "")

        response = get_with_retries(
            session,
            data_js_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"

        rows = _extract_rows_from_technical_paper_data(response.text or "", division_map)

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()
        for row in rows:
            if len(out) >= max_total_records:
                break

            candidate = _canonicalize(urljoin(page_url, row.href))
            if not candidate:
                continue
            if path_ext(candidate) != ".pdf":
                continue
            if candidate in seen_urls:
                continue

            meta = {
                "discovered_from": page_url,
                "division": row.division,
            }

            out.append(
                ctx.make_record(
                    url=candidate,
                    name=row.name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta=meta,
                    publish_date=row.publish_date,
                )
            )
            seen_urls.add(candidate)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("division") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out