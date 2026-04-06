from __future__ import annotations

import random
import re
import base64
import json
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
    "https://www.dsd.gov.hk/EN/Technical_Documents/Technical_Manuals/index.html"
)
_DEFAULT_TC_PAGE_URL = (
    "https://www.dsd.gov.hk/TC/Technical_Documents/Technical_Manuals/index.html"
)
_DEFAULT_DATA_JS_URL_EN = "https://www.dsd.gov.hk/assets/js/EN/technicalmanual-data-compress.js"
_DEFAULT_DATA_JS_URL_TC = "https://www.dsd.gov.hk/assets/js/TC/technicalmanual-data-compress.js"
_CORRIGENDUM_RE = re.compile(
    r"(?:corrigendum\s*no\.?|勘誤|更正)\s*(\d{1,2})\s*/\s*(\d{4})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _RowData:
    name: str
    links: list[tuple[str, str]]


_DATA_BLOB_RE = re.compile(r"technicalManualData\s*=\s*'([^']+)'", re.IGNORECASE)


def _extract_rows_from_compressed_data(
    js_text: str,
    page_url: str,
    session: requests.Session,
) -> list[_RowData]:
    blob_match = _DATA_BLOB_RE.search(js_text)
    if not blob_match:
        return []

    try:
        compressed = base64.b64decode(blob_match.group(1))
        decompressed = zlib.decompress(compressed, -zlib.MAX_WBITS)
        items = json.loads(decompressed.decode("utf-8"))
    except (ValueError, zlib.error, json.JSONDecodeError):
        return []

    rows: list[_RowData] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        title = clean_text(str(item.get("title") or ""))
        href = clean_text(str(item.get("url") or ""))
        caption = clean_text(str(item.get("download_caption") or ""))
        if not title or not href:
            continue

        rows.append(_RowData(name=title, links=[(href, caption)]))

    return rows


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_corrigendum_publish_date(name: str) -> str | None:
    match = _CORRIGENDUM_RE.search(name)
    if not match:
        return None

    month = int(match.group(1))
    year = int(match.group(2))
    if not (1 <= month <= 12):
        return None

    return normalize_publish_date(f"{year:04d}-{month:02d}-01")


class Crawler:
    name = "technical_manuals"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        locales_cfg = cfg.get("locales")
        if not isinstance(locales_cfg, list) or not locales_cfg:
            locales_cfg = [
                {
                    "locale": "en",
                    "page_url": str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip(),
                    "data_js_url": str(
                        cfg.get("data_js_url", _DEFAULT_DATA_JS_URL_EN)
                    ).strip(),
                },
                {
                    "locale": "tc",
                    "page_url": _DEFAULT_TC_PAGE_URL,
                    "data_js_url": _DEFAULT_DATA_JS_URL_TC,
                },
            ]

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

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for locale_entry in locales_cfg:
            if len(out) >= max_total_records:
                break

            if not isinstance(locale_entry, dict):
                continue

            locale = clean_text(str(locale_entry.get("locale") or "")).lower()
            page_url = clean_text(str(locale_entry.get("page_url") or ""))
            data_js_url = clean_text(str(locale_entry.get("data_js_url") or ""))
            if locale not in {"en", "tc"}:
                continue
            if not page_url or not data_js_url:
                continue

            response = get_with_retries(
                session,
                data_js_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            response.encoding = "utf-8"
            js_text = response.text or ""
            rows = _extract_rows_from_compressed_data(js_text, page_url, session)

            for row in rows:
                if len(out) >= max_total_records:
                    break

                chosen_pdf_url: str | None = None
                for href, _link_text in row.links:
                    candidate = _canonicalize(urljoin(page_url, href))
                    if not candidate:
                        continue
                    if path_ext(candidate) != ".pdf":
                        continue
                    chosen_pdf_url = candidate
                    break

                if not chosen_pdf_url:
                    continue

                if chosen_pdf_url in seen_urls:
                    continue

                publish_date = _extract_corrigendum_publish_date(row.name)
                meta: dict[str, str] = {
                    "discovered_from": page_url,
                    "locale": locale,
                }

                out.append(
                    ctx.make_record(
                        url=chosen_pdf_url,
                        name=row.name,
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
                str(r.meta.get("locale") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
