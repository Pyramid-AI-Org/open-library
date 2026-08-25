"""PUB, Singapore's National Water Agency - codes of practice.

The binding drainage and sewerage codes: the Code of Practice on Surface
Water Drainage, the Code of Practice on Sewerage and Sanitary Works, the
Code of Practice on Coastal Protection, and the standard drawings issued
with them. These are what a qualified person designs to, and they are the
closest thing PUB has to the DSD technical circulars.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.pub.pages.codes_of_practice`, which tracks
row `codes_of_practice` of `sources/sg-pub/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "codes_of_practice"
