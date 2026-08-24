"""Energy Market Authority - statistics.

Half-hourly and monthly market data and the Singapore Energy Statistics
chapters. Server-rendered, so a section crawl reaches it.

The matching persisted query, /corporate/statistics-list, answers HTTP 500. That
is a server fault, not an empty collection, and it must not be read as zero.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.statistics`, which tracks row `statistics` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "statistics"
