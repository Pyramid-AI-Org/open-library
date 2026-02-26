from __future__ import annotations

from datetime import datetime, timezone
import requests

from crawlers.base import RunContext, UrlRecord
from utils.html_links import extract_links, filter_links


class Crawler:
    """Proof-of-concept crawler.

    Fetches a single page and extracts anchor links from it.
    Config: crawlers.link_extract.page_url + optional filters.
    """

    name = "link_extract"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        page_url = cfg.get("page_url")
        if not page_url:
            return []

        text_contains = cfg.get("text_contains")
        href_contains = cfg.get("href_contains")
        limit = int(cfg.get("limit", 50))

        resp = requests.get(
            page_url,
            timeout=int(ctx.get_http_config().get("timeout_seconds", 30)),
            headers={"User-Agent": ctx.get_http_config().get("user_agent", "")},
        )
        resp.raise_for_status()

        links = extract_links(resp.text, base_url=page_url)
        links = filter_links(
            links, text_contains=text_contains, href_contains=href_contains
        )

        # Dedup + deterministic ordering
        seen: set[str] = set()
        records: list[UrlRecord] = []
        for link in sorted(links, key=lambda l: (l.href or "")):
            if link.href in seen:
                continue
            seen.add(link.href)
            records.append(
                ctx.make_record(
                    url=link.href,
                    name=link.text or None,
                    discovered_at_utc=datetime.now(timezone.utc).isoformat(),
                    source=self.name,
                    meta={"page_url": page_url},
                )
            )
            if len(records) >= limit:
                break

        return records
