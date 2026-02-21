from __future__ import annotations

import logging
import random
from urllib.parse import urlparse

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
from utils.html_links import extract_links, extract_links_in_element

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_URL = "https://bestpractice.emsd.gov.hk/en/booklet"
_DEFAULT_BOOKLET_ROOT = "https://bestpractice.emsd.gov.hk"


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "emsd.publications_handbooks"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        booklet_root = str(cfg.get("booklet_root", _DEFAULT_BOOKLET_ROOT)).rstrip("/")
        content_element_id = str(cfg.get("content_element_id", "bookletlistpage")).strip()

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

        try:
            landing_html = _fetch(page_url)
        except Exception as exc:
            logger.error(f"[{self.name}] Failed to fetch landing page {page_url}: {exc}")
            return []

        landing_links = extract_links_in_element(
            landing_html,
            base_url=page_url,
            element_id=content_element_id,
        )
        if not landing_links:
            landing_links = extract_links(landing_html, base_url=page_url)

        booklet_pages: list[tuple[str, str]] = []
        seen_booklet_pages: set[str] = set()

        for link in landing_links:
            can = _canonicalize(link.href)
            if not can:
                continue
            if not can.startswith(booklet_root + "/"):
                continue

            parsed = urlparse(can)
            if parsed.query:
                continue
            if path_ext(can):
                # Skip direct files (images/pdf/etc) on the landing page.
                continue
            if can == _canonicalize(page_url):
                continue
            if can in seen_booklet_pages:
                continue

            label = clean_text(link.text)
            if not label:
                continue

            booklet_pages.append((can, label))
            seen_booklet_pages.add(can)

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        for booklet_url, booklet_name in booklet_pages:
            if len(out) >= max_total_records:
                break

            try:
                booklet_html = _fetch(booklet_url)
            except Exception as exc:
                logger.error(f"[{self.name}] Failed to fetch booklet page {booklet_url}: {exc}")
                continue

            for link in extract_links(booklet_html, base_url=booklet_url):
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
                        name=booklet_name,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={"discovered_from": booklet_url},
                    )
                )
                seen_pdf_urls.add(can)

                if len(out) >= max_total_records:
                    break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(
            f"[{self.name}] Found {len(out)} handbook PDFs from {len(booklet_pages)} booklet pages"
        )
        return out
