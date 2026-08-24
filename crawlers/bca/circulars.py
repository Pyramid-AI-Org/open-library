"""BCA Circulars crawler.

Circulars are the running record of how BCA's regulations are applied, so they
matter as much as the codes themselves. The listing shows ten at a time behind
forty pages of pagination, but every item is present in the flight payload of
the first response - see `crawlers.common.isomer.IsomerCollectionCrawler`.

Scope lives in `config/settings.yaml` under `crawlers.bca.pages.circulars`,
which tracks row `circulars` of `sources/sg-bca/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.isomer import IsomerCollectionCrawler


class Crawler(IsomerCollectionCrawler):
    name = "circulars"
