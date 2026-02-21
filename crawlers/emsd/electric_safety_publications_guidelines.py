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
    name = "emsd.electric_safety_publications_guidelines"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        # 1. Load config
        crawler_cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        page_url = crawler_cfg.get(
            "page_url",
            "https://www.emsd.gov.hk/en/electricity_safety/publications/guidance_notes_guidelines/index.html"
        )
        content_element_id = crawler_cfg.get("content_element_id", "content")
        
        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = http_cfg.get("user_agent")
        max_retries = int(http_cfg.get("max_retries", 3))
        
        request_delay = float(crawler_cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(crawler_cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(crawler_cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(crawler_cfg.get("backoff_jitter_seconds", 0.25))

        # 2. Setup session
        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})
            
        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0, request_jitter))

        # 3. Fetch
        try:
            resp = get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
        except Exception as e:
            logger.error(f"Failed to fetch {page_url}: {e}")
            return []
            
        # 4. Extract links (scoped with fallback)
        html = _strip_html_comments(resp.text)
        links = extract_links_in_element(html, base_url=page_url, element_id=content_element_id)
        if not links:
            logger.warning(f"No links found in #{content_element_id} for {page_url}, falling back to full page scan")
            links = extract_links(html, base_url=page_url)

        records = []
        for link in links:
            # Filter: must be PDF or a sub-page in the same section
            parsed = urlparse(link.href)
            path_lower = parsed.path.lower()
            
            # 1. PDF files
            is_pdf = path_lower.endswith(".pdf")
            
            # 2. HTML sub-pages (e.g. guidelines details)
            # Avoid the index page itself
            is_subpage = (
                "/guidance_notes_guidelines/" in path_lower 
                and not path_lower.endswith("/guidance_notes_guidelines/index.html")
                and not path_lower.endswith("/guidance_notes_guidelines/")
            )
            
            if not (is_pdf or is_subpage):
                continue
            
            # Use link text or fallback to filename
            text = link.text.strip()
            if not text:
                text = parsed.path.split("/")[-1]

            records.append(UrlRecord(
                url=link.href,
                name=text,
                discovered_at_utc=ctx.started_at_utc,
                source=self.name,
                meta={
                    "discovered_from": page_url
                }
            ))
            
        logger.info(f"Found {len(records)} PDF records from {page_url}")
        return records
