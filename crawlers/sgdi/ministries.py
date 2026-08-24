"""Singapore Government Directory Interactive - ministries.

The 16 ministries and, beneath each, its departments, statutory boards and
committees, down to the officer records. Three levels deep, all server-
rendered, no pagination anywhere.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.sgdi.pages.ministries`, which tracks
row `ministries` of `sources/sg-sgdi/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "ministries"
