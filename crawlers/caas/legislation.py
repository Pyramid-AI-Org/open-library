"""CAAS - aviation legislation.

The Acts and subsidiary legislation the advisory circulars are written under,
including the Air Navigation Act, whose obstacle provisions are the statutory
basis for height control.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.sg.yaml` under `crawlers.caas.pages.legislation`, which
tracks row `legislation` of `sources/sg-caas/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "legislation"
