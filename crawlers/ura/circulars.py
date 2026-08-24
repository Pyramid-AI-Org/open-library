"""Urban Redevelopment Authority - circulars.

URA circulars are the closest thing to the Hong Kong practice notes: how
development control requirements change and how they are applied. 61 items
across development control, urban design, conservation and Controller of
Housing.

Behaviour lives in `crawlers.common.sitemap.CollectionListingCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ura.pages.circulars`, which tracks
row `circulars` of `sources/sg-ura/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import CollectionListingCrawler


class Crawler(CollectionListingCrawler):
    name = "circulars"
