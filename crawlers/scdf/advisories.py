"""Singapore Civil Defence Force - advisories.

Fire safety and emergency advisories - the notices and guidance row of the
Hong Kong deck.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.scdf.pages.advisories`, which tracks
row `advisories` of `sources/sg-scdf/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "advisories"
