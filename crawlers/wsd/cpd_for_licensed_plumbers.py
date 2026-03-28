from __future__ import annotations

from crawlers.base import RunContext, UrlRecord
from crawlers.wsd.common import TraversalMode, crawl_page_tree


_DEFAULT_PAGE_URL = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "voluntary-continuing-professional-development-sche/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "voluntary-continuing-professional-development-sche"
)


class Crawler:
    name = "cpd_for_licensed_plumbers"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        scope_prefix = (
            str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip().rstrip("/")
        )
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        max_pages = int(cfg.get("max_pages", 200))
        max_out_links_per_page = int(cfg.get("max_out_links_per_page", 500))
        max_total_records = int(cfg.get("max_total_records", 50000))

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        return crawl_page_tree(
            ctx,
            source_name=self.name,
            page_url=page_url,
            scope_prefix=scope_prefix,
            content_element_id=content_element_id,
            max_pages=max_pages,
            max_out_links_per_page=max_out_links_per_page,
            max_total_records=max_total_records,
            request_delay=request_delay,
            request_jitter=request_jitter,
            backoff_base=backoff_base,
            backoff_jitter=backoff_jitter,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            max_retries=max_retries,
            mode=TraversalMode(
                emit_page_records=False,
                emit_pdf_records=True,
                emit_pdf_from_seed=True,
                emit_pdf_from_subpages=True,
                include_seed_page_record=False,
                seed_page_discovered_from_self=False,
            ),
        )
