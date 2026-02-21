from __future__ import annotations

import logging
import random

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

_DEFAULT_PAGE_URL = "https://www.emsd.gov.hk/en/gas_safety/approval_schemes/index.html"
_DEFAULT_SCOPE_PREFIX = "https://www.emsd.gov.hk/en/gas_safety/approval_schemes/"


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "emsd.gas_approval_schemes"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        scope_prefix = str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip()
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

        def _extract_links(html: str, *, base_url: str):
            links = extract_links_in_element(
                html,
                base_url=base_url,
                element_id=content_element_id,
            )
            if not links:
                links = extract_links(html, base_url=base_url)
            return links

        main_html = _fetch(page_url)
        main_links = _extract_links(main_html, base_url=page_url)

        subpages: list[str] = []
        seen_subpages: set[str] = set()
        for link in main_links:
            can = _canonicalize(link.href)
            if not can:
                continue
            if not can.startswith(scope_prefix) or can == page_url:
                continue
            if not can.endswith("/index.html"):
                continue
            if can in seen_subpages:
                continue
            subpages.append(can)
            seen_subpages.add(can)

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        for subpage_url in subpages:
            if len(out) >= max_total_records:
                break

            try:
                sub_html = _fetch(subpage_url)
            except Exception as exc:
                logger.error(f"[{self.name}] Failed to fetch subpage {subpage_url}: {exc}")
                continue

            for link in _extract_links(sub_html, base_url=subpage_url):
                can = _canonicalize(link.href)
                if not can:
                    continue
                if path_ext(can) != ".pdf":
                    continue
                if can in seen_pdf_urls:
                    continue

                out.append(
                    UrlRecord(
                        url=can,
                        name=clean_text(link.text) or infer_name_from_link(link.text, can),
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={"discovered_from": subpage_url},
                    )
                )
                seen_pdf_urls.add(can)

                if len(out) >= max_total_records:
                    break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(f"[{self.name}] Found {len(out)} PDF URLs from {len(subpages)} sub-pages")
        return out
