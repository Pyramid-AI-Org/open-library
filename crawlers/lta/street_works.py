"""LTA street works requirements crawler.

Codes of practice for street work proposals, works on public streets and
traffic control at work zones, plus the transport impact assessment guidelines
and the forms and checklists that accompany them.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives
in `config/settings.yaml` under `crawlers.lta.pages.street_works`,
which tracks row `street_works` of `sources/sg-lta/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "street_works"
