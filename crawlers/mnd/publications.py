"""Ministry of National Development - publications.

MND's published policy documents - the sustainability blueprint, the climate
action plan and the urban sustainability series. Thin, and policy rather
than technical, but it is what this ministry publishes.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.mnd.pages.publications`, which tracks
row `publications` of `sources/sg-mnd/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "publications"
