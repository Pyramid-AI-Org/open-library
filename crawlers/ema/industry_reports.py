"""Energy Market Authority - industry reports.

Market and system studies EMA publishes for industry planning.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.industry_reports`, which tracks row `industry_reports` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "industry_reports"
