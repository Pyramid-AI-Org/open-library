"""CAAS - airport and aerodrome regulatory requirements.

Advisory circulars and codes of practice for aerodrome design and operation.
The construction-relevant material here is the handling-of-obstructions and
terminal-safety guidance: an aerodrome's obstacle limitation surfaces decide
how tall a building near the airport may be, which binds real projects well
beyond the airport boundary.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.sg.yaml` under `crawlers.caas.pages.airport_and_aerodrome`,
which tracks row `airport_and_aerodrome` of `sources/sg-caas/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "airport_and_aerodrome"
