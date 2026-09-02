"""BD codes-and-design-manuals sub-sections.

The codes-and-design-manuals index links out to sub-sections — minor works
general and technical guidance, building maintenance guidebook, supervision of
site works — whose documents appear nowhere on the index itself. 97 documents
sat behind those four links, unreachable.

They are a separate section rather than extra pages on the parent because the
parent's parser is built around its own div-based index layout and returns
nothing on these pages, where documents sit in table rows. This uses the
scheduled-areas crawler, which takes every PDF inside the content element.
"""

from __future__ import annotations

from crawlers.bd.scheduled_areas import Crawler as _ContentPdfCrawler
from crawlers.base import RunContext, UrlRecord


class Crawler(_ContentPdfCrawler):
    name = "codes_and_design_manuals_subsections"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        page_urls = [str(u).strip() for u in (cfg.get("page_urls") or []) if str(u).strip()]

        out: list[UrlRecord] = []
        seen: set[str] = set()
        for page_url in page_urls:
            # The parent reads a single `page_url`; run it once per sub-section.
            section_ctx = _SinglePageContext(ctx, self.name, page_url)
            try:
                records = super().crawl(section_ctx)
            except Exception as exc:
                if ctx.debug:
                    print(f"[{self.name}] Skip page fetch failure: {page_url} ({exc})")
                continue
            for rec in records:
                if rec.url in seen:
                    continue
                seen.add(rec.url)
                out.append(rec)

        out.sort(key=lambda r: (r.url, r.name or ""))
        return out


class _SinglePageContext:
    """Wraps RunContext so the parent crawler sees one page_url at a time."""

    def __init__(self, ctx: RunContext, name: str, page_url: str) -> None:
        self._ctx = ctx
        self._name = name
        self._page_url = page_url

    def get_crawler_config(self, crawler_name: str) -> dict:
        cfg = dict(self._ctx.get_crawler_config(self._name))
        cfg.pop("page_urls", None)
        cfg["page_url"] = self._page_url
        return cfg

    def __getattr__(self, item):
        return getattr(self._ctx, item)
