from __future__ import annotations

import logging
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    clean_text,
    get_with_retries,
    sleep_seconds,
)
from utils.html_links import extract_links

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.emsd.gov.hk/gsp/en/"
_JS_DATA_URL = "https://www.emsd.gov.hk/gsp/js/icon_template_en.js"


class _MainPageParser(HTMLParser):
    """Parses index.html to find sub-pages inside div.pageIcon > a."""

    def __init__(self) -> None:
        super().__init__()
        self.found_links: list[tuple[str, str]] = []
        self._in_page_icon = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "div":
            # Check for <div class="... pageIcon ...">
            classes = ""
            for k, v in attrs:
                if k == "class" and v:
                    classes = v
            if "pageIcon" in classes.split():
                self._in_page_icon = True
                return

        if self._in_page_icon and tag == "a":
            href = None
            title = None
            for k, v in attrs:
                if k == "href":
                    href = v
                elif k == "title":
                    title = v

            if href:
                # We assume title attribute is good enough as name, 
                # otherwise fetch text if title is empty.
                self.found_links.append((href, title or ""))
            
            # Reset after finding the link in this block
            self._in_page_icon = False

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._in_page_icon:
            self._in_page_icon = False


class Crawler:
    name = "emsd.gas_safety_portal"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        crawler_cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        backoff_base = float(crawler_cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(crawler_cfg.get("backoff_jitter_seconds", 0.25))

        request_delay = float(crawler_cfg.get("request_delay_seconds", 0.5))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        # 1. Fetch Main Page Data (embedded in JS)
        logger.info(f"[{self.name}] Fetching data from JS: {_JS_DATA_URL}")
        resp = get_with_retries(
            session,
            _JS_DATA_URL,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )

        # Basic JS string extraction:
        # Expected format: var iconSet = '...';
        # Lines end with backslash for continuation.
        raw_js = resp.text
        # Remove line continuations
        clean_js = raw_js.replace("\\\n", "").replace("\\\r\n", "")
        
        # Extract the HTML string inside the quotes
        # Find first ' and last '
        start_idx = clean_js.find("'")
        end_idx = clean_js.rfind("'")
        
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            html_content = clean_js[start_idx + 1:end_idx]
        else:
            logger.error(f"[{self.name}] Could not extract HTML from JS")
            html_content = ""

        parser = _MainPageParser()
        parser.feed(html_content)

        # Exclude last 4 items as requested
        # "Ignore last 4"
        all_links = parser.found_links
        if len(all_links) > 4:
            target_links = all_links[:-4]
        else:
            target_links = all_links  # fallback if structure changes drastically

        records: list[UrlRecord] = []

        for href, title in target_links:
            full_url = urljoin(_BASE_URL, href)
            name = clean_text(title)

            # 2. Check for special b07.html
            if "b07.html" in full_url:
                logger.info(f"[{self.name}] Expanding special page: {full_url}")
                # Fetch b07 and extract PDFs
                if request_delay > 0:
                    sleep_seconds(request_delay)

                try:
                    sub_resp = get_with_retries(
                        session,
                        full_url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base,
                        backoff_jitter_seconds=backoff_jitter,
                    )
                    # Extract links from b07.html
                    # We look for PDF links specifically as requested
                    # "We shouldn't keep url of sub-page, instead get that pdf urls with name"
                    sub_links = extract_links(sub_resp.text, base_url=full_url)
                    found_pdfs = 0
                    for link in sub_links:
                        link_href_lower = link.href.lower() if link.href else ""
                        if link_href_lower.endswith(".pdf"):
                            records.append(
                                UrlRecord(
                                    url=link.href,
                                    name=clean_text(link.text) or "PDF Document",
                                    discovered_at_utc=ctx.run_date_utc,
                                    source=self.name,
                                    meta={"category": name},
                                )
                            )
                            found_pdfs += 1
                    
                    if found_pdfs == 0:
                        logger.warning(f"[{self.name}] No PDFs found in {full_url}")

                except Exception as e:
                    logger.error(f"[{self.name}] Failed to process {full_url}: {e}")

            else:
                # Normal page, just record it
                records.append(
                    UrlRecord(
                        url=full_url,
                        name=name,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={},
                    )
                )

        return records
