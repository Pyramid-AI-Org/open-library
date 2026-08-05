from __future__ import annotations

import logging
import random
import re
from urllib.parse import unquote, urljoin, urlparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element

logger = logging.getLogger(__name__)

_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)
_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
# Live BEEO cards render each document as an image link whose <img alt="…">
# carries the human title, e.g. alt="Download BEC 2024 Edition".
_ALT_TITLE_RE = re.compile(
    r"<a[^>]+href=\"([^\"]*\.pdf)\"[^>]*>\s*<img[^>]*\balt=\"([^\"]*)\"",
    flags=re.IGNORECASE,
)
_DOWNLOAD_PREFIX_RE = re.compile(r"^\s*download\s+", flags=re.IGNORECASE)


def _strip_html_comments(html: str) -> str:
    if not html:
        return ""
    return _HTML_COMMENT_RE.sub("", html)


def _build_alt_title_map(html: str, base_url: str) -> dict[str, str]:
    """Map each PDF URL to the title carried in its image link's alt text."""
    titles: dict[str, str] = {}
    for href, alt in _ALT_TITLE_RE.findall(html):
        title = clean_text(_DOWNLOAD_PREFIX_RE.sub("", alt))
        if not title:
            continue
        titles.setdefault(urljoin(base_url, href), title)
    return titles


def _infer_year(link_text: str, href: str) -> str | None:
    """Pick the latest 4-digit year from the link text or filename.

    BEEO documents encode their edition year in the title/filename (e.g.
    "BEC 2021 Edition", "BEC_2024_ENG.pdf"). Comparison docs mention two years
    (e.g. BEC2021vsBEC2024); the newer year is the more useful edition marker.
    """
    filename = unquote(urlparse(href).path.rsplit("/", 1)[-1])
    years = _YEAR_RE.findall(f"{link_text} {filename}")
    if not years:
        return None
    return max(years)


class Crawler:
    name = "beeo_codes_guidelines"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        # 1. Load config
        crawler_cfg = ctx.get_crawler_config(self.name)
        page_url = crawler_cfg.get(
            "page_url",
            "https://www.emsd.gov.hk/beeo/en/mibec_beeo_codtechguidelines.html",
        )
        content_element_id = crawler_cfg.get("content_element_id", "content")
        scope_prefix = crawler_cfg.get(
            "scope_prefix", "https://www.emsd.gov.hk/beeo/"
        )

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = http_cfg.get("user_agent")
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(crawler_cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(crawler_cfg.get("request_jitter_seconds", 0.10))
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

        # 4. Extract links (scoped with fallback to full page)
        html = _strip_html_comments(resp.text)
        links = extract_links_in_element(
            html, base_url=page_url, element_id=content_element_id
        )
        if not links:
            logger.warning(
                f"No links found in #{content_element_id} for {page_url}, "
                "falling back to full page scan"
            )
            links = extract_links(html, base_url=page_url)

        # 5. Titles come from each card's <img alt="Download …"> text.
        alt_titles = _build_alt_title_map(html, page_url)

        # 6. Keep PDFs within the BEEO scope, deduped by URL.
        # Some hrefs contain literal spaces; the origin only serves the
        # percent-encoded form, so normalize spaces for reliable downloads.
        seen: set[str] = set()
        records: list[UrlRecord] = []
        for link in links:
            href = link.href
            parsed = urlparse(href)
            if not parsed.path.lower().endswith(".pdf"):
                continue
            if scope_prefix and not href.startswith(scope_prefix):
                continue

            title = alt_titles.get(href) or link.text.strip()
            url = href.replace(" ", "%20")
            if url in seen:
                continue
            seen.add(url)

            name = infer_name_from_link(title, href)
            publish_date = _infer_year(title, href)

            records.append(
                ctx.make_record(
                    url=url,
                    name=name,
                    discovered_at_utc=ctx.started_at_utc,
                    publish_date=publish_date,
                    source=self.name,
                    meta={"discovered_from": page_url},
                )
            )

        logger.info(f"Found {len(records)} PDF records from {page_url}")
        return records
