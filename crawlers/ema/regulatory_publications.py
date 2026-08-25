"""Energy Market Authority - regulatory publications.

EMA's technical library: the Gas Safety Code, the Transmission Code, the Gas
Metering Code and the policy papers issued under them. The direct equivalent of
the EMSD electricity and gas safety codes.

The spider that used to serve this row returned zero. The listing is Angular;
the collection comes back whole from one persisted query.

Behaviour lives in `crawlers.common.payload.ApiIndexCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.regulatory_publications`, which tracks row `regulatory_publications` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ApiIndexCrawler


class Crawler(ApiIndexCrawler):
    name = "regulatory_publications"
