"""Urban Redevelopment Authority - development control.

The 17 use-type development control handbooks plus the gross floor area
handbook - Singapore's equivalent of the Hong Kong Planning Standards and
Guidelines chapters.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ura.pages.development_control`, which tracks
row `development_control` of `sources/sg-ura/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "development_control"
