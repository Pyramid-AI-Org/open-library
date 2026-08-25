"""Urban Redevelopment Authority - conservation.

Conservation guidelines and the technical handbooks issued with them -
envelope control, additions and alterations, historic districts, the Do-It-
Right guides and the shophouse and bungalow technical supplements.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ura.pages.conservation`, which tracks
row `conservation` of `sources/sg-ura/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "conservation"
