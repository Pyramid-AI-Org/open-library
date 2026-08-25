"""Energy Market Authority - corporate publications.

Annual reports and the Singapore Energy Statistics series. Borderline corporate
literature, kept because the statistics volumes are cited as reference data.

Behaviour lives in `crawlers.common.payload.ApiIndexCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.corporate_publications`, which tracks row `corporate_publications` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ApiIndexCrawler


class Crawler(ApiIndexCrawler):
    name = "corporate_publications"
