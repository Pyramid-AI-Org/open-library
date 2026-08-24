"""LTA vehicle parking requirements crawler.

The Code of Practice on Vehicle Parking Provision in Development Proposals and
the material issued with it.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives
in `config/settings.yaml` under `crawlers.lta.pages.vehicle_parking`,
which tracks row `vehicle_parking` of `sources/sg-lta/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "vehicle_parking"
