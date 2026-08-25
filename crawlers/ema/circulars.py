"""Energy Market Authority - circulars.

The circular series - how EMA amends and interprets the codes between editions.
The persisted query takes an optional date filter; called without one it returns
the whole series.

Behaviour lives in `crawlers.common.payload.ApiIndexCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.circulars`, which tracks row `circulars` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ApiIndexCrawler


class Crawler(ApiIndexCrawler):
    name = "circulars"
