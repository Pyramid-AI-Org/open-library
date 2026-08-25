"""Housing & Development Board - town design guides.

One design guide per town, setting the urban design and architectural
standards for public housing estates - the closest HDB analogue to the
ArchSD design manuals.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.hdb.pages.town_design_guides`, which tracks
row `town_design_guides` of `sources/sg-hdb/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "town_design_guides"
