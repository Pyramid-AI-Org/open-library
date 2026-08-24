"""LTA railway protection and road structure safety zone crawler.

Requirements for developments within railway protection and road structure
safety zones - the constraints that bind anyone building near an MRT line or a
road structure.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives
in `config/settings.yaml` under `crawlers.lta.pages.railway_protection`,
which tracks row `railway_protection` of `sources/sg-lta/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "railway_protection"
