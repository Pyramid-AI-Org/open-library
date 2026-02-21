from __future__ import annotations

import logging
import random
import re
from urllib.parse import urlparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    get_with_retries,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element

logger = logging.getLogger(__name__)

_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)


def _strip_html_comments(html: str) -> str:
    if not html:
        return ""
    return _HTML_COMMENT_RE.sub("", html)


class Crawler:
    name = "emsd.regulating_railway_safety"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        crawler_cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        # Start URL from config or default
        start_url = str(
            crawler_cfg.get(
                "start_url",
                "https://www.emsd.gov.hk/en/railway_safety/regulating_railway_safety/index.html",
            )
        ).strip()

        # Prefix required for sub-pages
        prefix = "https://www.emsd.gov.hk/en/railway_safety/regulating_railway_safety"

        content_element_id = str(
            crawler_cfg.get("content_element_id", "content")
        ).strip()

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(crawler_cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(crawler_cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(crawler_cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(crawler_cfg.get("backoff_jitter_seconds", 0.25))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        queue = [start_url]
        visited_urls_normalized = set()

        records: list[UrlRecord] = []

        while queue:
            current_url = queue.pop(0)

            # Normalize URL for visited check (remove fragment)
            normalized_url = current_url.split("#")[0]
            if normalized_url in visited_urls_normalized:
                continue
            visited_urls_normalized.add(normalized_url)

            # Delay before fetch
            if request_delay > 0:
                sleep_seconds(request_delay + random.uniform(0, request_jitter))

            try:
                if ctx.debug:
                    logger.info(f"[{self.name}] Fetching {normalized_url}")

                resp = get_with_retries(
                    session,
                    normalized_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base,
                    backoff_jitter_seconds=backoff_jitter,
                )
            except Exception as e:
                logger.error(f"[{self.name}] Failed to fetch {normalized_url}: {e}")
                continue

            # Strip comments
            html = _strip_html_comments(resp.text)

            # Extract links
            # Try scoped to #content first, fall back to full page
            links = extract_links_in_element(
                html, base_url=normalized_url, element_id=content_element_id
            )
            if not links:
                if ctx.debug:
                    logger.warning(
                        f"[{self.name}] No links found in #{content_element_id} for {normalized_url}, falling back to full page scan"
                    )
                links = extract_links(html, base_url=normalized_url)

            for link in links:
                abs_url = link.href
                parsed = urlparse(abs_url)
                path_lower = parsed.path.lower()

                # Case 1: PDF -> Record
                if path_lower.endswith(".pdf"):
                    text = link.text.strip()
                    if not text:
                        text = parsed.path.split("/")[-1]

                    records.append(
                        UrlRecord(
                            url=abs_url,
                            name=text,
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta={"discovered_from": normalized_url},
                        )
                    )

                # Case 2: Sub-page -> Queue
                # Must start with the required prefix
                elif abs_url.startswith(prefix):
                    sub_normalized = abs_url.split("#")[0]

                    if (
                        sub_normalized not in visited_urls_normalized
                        and sub_normalized not in queue
                    ):
                        queue.append(sub_normalized)

        logger.info(
            f"[{self.name}] Crawled {len(visited_urls_normalized)} pages, found {len(records)} PDFs"
        )
        return records
