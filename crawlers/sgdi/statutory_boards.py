"""Singapore Government Directory Interactive - statutory boards.

All 73 statutory boards, listed on a single page.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.sgdi.pages.statutory_boards`, which tracks
row `statutory_boards` of `sources/sg-sgdi/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "statutory_boards"
