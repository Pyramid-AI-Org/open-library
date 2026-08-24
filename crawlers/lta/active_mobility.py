"""LTA active mobility requirements crawler.

The Active Mobility Design Guide, its checklist, and the LTA circulars issued
with them - the walking-and-cycling design standards for developments.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives
in `config/settings.yaml` under `crawlers.lta.pages.active_mobility`,
which tracks row `active_mobility` of `sources/sg-lta/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "active_mobility"
