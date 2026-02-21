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

_DEFAULT_PAGE_URL = (
    "https://www.emsd.gov.hk/en/gas_safety/related_information_for_competent_person/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.emsd.gov.hk/en/gas_safety/related_information_for_competent_person/"
)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "emsd.gas_related_information_for_competent_person"

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

        def _extract_page_links(html: str, *, base_url: str):
            links = extract_links_in_element(
                html,
                base_url=base_url,
                element_id=content_element_id,
            )
            if not links:
                links = extract_links(html, base_url=base_url)
            return links

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        main_html = _fetch(page_url)
        main_links = _extract_page_links(main_html, base_url=page_url)

        subpages: list[str] = []
        seen_subpages: set[str] = set()

        def _emit_pdf(url: str, name: str | None, discovered_from: str) -> None:
            can = _canonicalize(url)
            if not can:
                return
            if path_ext(can) != ".pdf":
                return
            if can in seen_pdf_urls:
                return
            out.append(
                UrlRecord(
                    url=can,
                    name=clean_text(name) or infer_name_from_link(name or "", can),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": discovered_from},
                )
            )
            seen_pdf_urls.add(can)

        # Collect PDFs from main page and discover subpages.
        for link in main_links:
            can = _canonicalize(link.href)
            if not can:
                continue

            if path_ext(can) == ".pdf":
                _emit_pdf(can, link.text, page_url)
            elif (
                can.startswith(scope_prefix)
                and can != page_url
                and can.endswith("/index.html")
                and can not in seen_subpages
            ):
                subpages.append(can)
                seen_subpages.add(can)

            if len(out) >= max_total_records:
                break

        # One-level depth: crawl only subpages directly found on main page.
        for subpage_url in subpages:
            if len(out) >= max_total_records:
                break

            try:
                sub_html = _fetch(subpage_url)
            except Exception as exc:
                logger.error(f"[{self.name}] Failed to fetch subpage {subpage_url}: {exc}")
                continue

            for link in _extract_page_links(sub_html, base_url=subpage_url):
                can = _canonicalize(link.href)
                if not can:
                    continue
                if path_ext(can) != ".pdf":
                    continue
                _emit_pdf(can, link.text, subpage_url)

                if len(out) >= max_total_records:
                    break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(f"[{self.name}] Found {len(out)} PDF URLs")
        return out
