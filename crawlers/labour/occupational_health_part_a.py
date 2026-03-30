from __future__ import annotations

from crawlers.base import RunContext, UrlRecord
from crawlers.labour.occupational_health_common import crawl_occupational_health_part


_DEFAULT_PAGE_URL_EN = "https://www.labour.gov.hk/eng/public/content2_9a.htm"
_DEFAULT_PAGE_URL_TC = "https://www.labour.gov.hk/tc/public/content2_9a.htm"


class Crawler:
    name = "occupational_health_part_a"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        return crawl_occupational_health_part(
            ctx=ctx,
            crawler_name=self.name,
            default_page_url_en=_DEFAULT_PAGE_URL_EN,
            default_page_url_tc=_DEFAULT_PAGE_URL_TC,
        )
