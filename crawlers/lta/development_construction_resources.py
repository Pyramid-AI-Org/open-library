"""LTA development and construction resources crawler.

The section hub: submission requirements, design templates, preliminaries for
construction works and the fee schedules. Its sub-sections are crawled by their
own rows, so this one stays on the hub itself.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives
in `config/settings.yaml` under `crawlers.lta.pages.development_construction_resources`,
which tracks row `development_construction_resources` of `sources/sg-lta/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "development_construction_resources"
