"""Energy Market Authority - educational materials.

Licensing handbooks and electrical and gas safety guidance - the applied half of
EMA's published material.

This is the one EMA collection with two routes to it. The plain REST servlet at
/bin/corporate-site/education-material carries publishdate fields the GraphQL
query does not, so it is listed first in settings.

Behaviour lives in `crawlers.common.payload.ApiIndexCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.educational_materials`, which tracks row `educational_materials` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ApiIndexCrawler


class Crawler(ApiIndexCrawler):
    name = "educational_materials"
