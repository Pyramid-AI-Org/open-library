"""Housing & Development Board - building professionals.

The consultants-and-contractors library: HDB(ARCH) technical requirements,
the civil and structural plan submission guidelines, the renovation and A&A
guidelines, the documents-and-checklists set, and the updates page that
functions as HDB's circular series.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.hdb.pages.building_professionals`, which tracks
row `building_professionals` of `sources/sg-hdb/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "building_professionals"
