"""National Environment Agency - publications.

Annual and sustainability reports and the environment yearbooks - the
publications row of the Hong Kong deck.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nea.pages.publications`, which tracks
row `publications` of `sources/sg-nea/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "publications"
