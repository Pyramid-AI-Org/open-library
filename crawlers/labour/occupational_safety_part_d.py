from __future__ import annotations

from crawlers.base import RunContext, UrlRecord
from crawlers.labour.occupational_safety_common import crawl_occupational_safety_part


_DEFAULT_PAGE_URL = "https://www.labour.gov.hk/eng/public/content2_8d.htm"


class Crawler:
    name = "occupational_safety_part_d"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        return crawl_occupational_safety_part(
            ctx=ctx,
            crawler_name=self.name,
            default_page_url=_DEFAULT_PAGE_URL,
        )
