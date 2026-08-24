"""Singapore Land Authority - circulars.

The direct Lands Department equivalent and the highest-value target on the
site: 332 circulars and notices from 1999 to the present. Chief Surveyor
notices and circulars, practice circulars, the Chief Surveyor directives and
guidelines, consolidated practice circulars, and the circulars to
professional institutes. Each row links straight to the whole PDF with no
intermediate page and no per-section split, which is exactly what the Hong
Kong note asked for.

Behaviour lives in `crawlers.common.sitemap.CollectionListingCrawler`; scope lives in
`config/settings.yaml` under `crawlers.sla.pages.circulars`, which tracks
row `circulars` of `sources/sg-sla/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import CollectionListingCrawler


class Crawler(CollectionListingCrawler):
    name = "circulars"
