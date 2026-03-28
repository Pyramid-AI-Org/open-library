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
    path_ext,
    sleep_seconds,
)

_DEFAULT_PAGE_URL = (
    "https://www.dsd.gov.hk/EN/Technical_Documents/DSD_Guidelines/index.html"
)
_DEFAULT_DATA_JS_URL = (
    "https://www.dsd.gov.hk/assets/js/EN/guideline-data-compress-47.js"
)

_GUIDELINE_BLOB_RE = re.compile(r"guidelineData\s*=\s*'([^']+)'", re.IGNORECASE)


@dataclass(frozen=True)
class _RowData:
    name: str
    href: str


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_rows_from_compressed_data(js_text: str) -> list[_RowData]:
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

        name = clean_text(str(item.get("title") or ""))
        href = clean_text(str(item.get("link") or ""))
        if not name or not href:
            continue

        out.append(_RowData(name=name, href=href))

    return out


class Crawler:
    name = "guidelines"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))
        data_js_url = clean_text(str(cfg.get("data_js_url") or _DEFAULT_DATA_JS_URL))

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

        response = get_with_retries(
            session,
            data_js_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"

        rows = _extract_rows_from_compressed_data(response.text or "")

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

            out.append(
                ctx.make_record(
                    url=candidate,
                    name=row.name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": page_url},
                )
            )
            seen_urls.add(candidate)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
