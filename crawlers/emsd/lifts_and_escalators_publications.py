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
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element

logger = logging.getLogger(__name__)

_DEFAULT_ROOT_URL = (
    "https://www.emsd.gov.hk/en/lifts_and_escalators_safety/publications/index.html"
)

_DEFAULT_SECTION_URLS = {
    "general_report": (
        "https://www.emsd.gov.hk/en/lifts_and_escalators_safety/publications/"
        "general_report/index.html"
    ),
    "guidance_notes_guidelines": (
        "https://www.emsd.gov.hk/en/lifts_and_escalators_safety/publications/"
        "guidance_notes_guidelines/index.html"
    ),
    "code_of_practice": (
        "https://www.emsd.gov.hk/en/lifts_and_escalators_safety/publications/"
        "code_of_practice/index.html"
    ),
}

_DEFAULT_NESTED_CRAWL_FLAGS = {
    "general_report": False,
    "guidance_notes_guidelines": True,
    "code_of_practice": True,
}


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _section_prefix(section_url: str) -> str:
    if section_url.endswith("/index.html"):
        return section_url.rsplit("/", 1)[0] + "/"
    if section_url.endswith("/"):
        return section_url
    return section_url.rsplit("/", 1)[0] + "/"


def _is_html_like(url: str) -> bool:
    ext = path_ext(url)
    return ext in {"", ".html", ".htm", ".php", ".asp", ".aspx"}


class Crawler:
    name = "emsd.lifts_and_escalators_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        root_url = _canonicalize(str(cfg.get("root_url", _DEFAULT_ROOT_URL)).strip())

        raw_section_urls = cfg.get("section_urls", {})
        section_urls: dict[str, str] = {}
        for key, default_url in _DEFAULT_SECTION_URLS.items():
            candidate = str(raw_section_urls.get(key, default_url)).strip()
            can = _canonicalize(candidate)
            if can:
                section_urls[key] = can

        raw_nested_flags = cfg.get("crawl_nested_subpages", {})
        nested_flags: dict[str, bool] = {}
        for key, default_flag in _DEFAULT_NESTED_CRAWL_FLAGS.items():
            nested_flags[key] = _as_bool(raw_nested_flags.get(key, default_flag))

        content_element_id = str(cfg.get("content_element_id", "content")).strip()
        max_nested_depth = max(0, int(cfg.get("max_nested_depth", 1)))

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
                sleep_seconds(
                    request_delay + random.uniform(0.0, max(0.0, request_jitter))
                )
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

        if root_url:
            try:
                _fetch(root_url)
            except Exception as exc:
                if ctx.debug:
                    logger.warning(f"[{self.name}] Failed to fetch root {root_url}: {exc}")

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        for section_key in _DEFAULT_SECTION_URLS:
            section_url = section_urls.get(section_key)
            if not section_url:
                continue

            section_host = urlparse(section_url).netloc.lower()
            section_prefix = _section_prefix(section_url)
            allow_nested = nested_flags.get(section_key, False)

            queue: list[tuple[str, int]] = [(section_url, 0)]
            seen_pages: set[str] = set()

            while queue:
                if len(out) >= max_total_records:
                    break

                page_url, depth = queue.pop(0)
                if page_url in seen_pages:
                    continue
                seen_pages.add(page_url)

                try:
                    html = _fetch(page_url)
                except Exception as exc:
                    logger.error(f"[{self.name}] Failed to fetch page {page_url}: {exc}")
                    continue

                for link in _extract_links(html, base_url=page_url):
                    can = _canonicalize(link.href)
                    if not can:
                        continue

                    if path_ext(can) == ".pdf":
                        # URL-based dedupe only. Keep first-seen metadata and name.
                        if can in seen_pdf_urls:
                            continue
                        out.append(
                            UrlRecord(
                                url=can,
                                name=clean_text(link.text)
                                or infer_name_from_link(link.text, can),
                                discovered_at_utc=ctx.run_date_utc,
                                source=self.name,
                                meta={
                                    "discovered_from": page_url,
                                    "section": section_key,
                                },
                            )
                        )
                        seen_pdf_urls.add(can)

                        if len(out) >= max_total_records:
                            break
                        continue

                    if not allow_nested:
                        continue
                    if depth >= max_nested_depth:
                        continue
                    if not _is_html_like(can):
                        continue
                    if can in seen_pages:
                        continue

                    parsed = urlparse(can)
                    if parsed.netloc.lower() != section_host:
                        continue
                    if not can.startswith(section_prefix):
                        continue
                    if can == page_url:
                        continue

                    if all(queued_url != can for queued_url, _ in queue):
                        queue.append((can, depth + 1))

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(f"[{self.name}] Found {len(out)} PDF URLs")
        return out
