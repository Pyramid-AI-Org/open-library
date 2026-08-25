"""Energy Market Authority - licences.

Licence conditions, application forms, codes of practice and the
determinations issued under them, for every class of electricity and gas
licensee. 504 documents - by volume this is the source, and none of it was
visible to the section spider that preceded this engine.

Behaviour lives in `crawlers.common.payload.ApiIndexCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.licences`, which tracks row `licences` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ApiIndexCrawler


class Crawler(ApiIndexCrawler):
    name = "licences"
