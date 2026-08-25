"""Singapore Land Authority - property boundaries.

Cadastral and property boundary survey requirements - the survey practice
half of the Lands Department mapping.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.sla.pages.property_boundaries`, which tracks
row `property_boundaries` of `sources/sg-sla/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "property_boundaries"
