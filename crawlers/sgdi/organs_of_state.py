"""Singapore Government Directory Interactive - organs of state.

The 10 organs of state - Attorney-General's Chambers, the courts,
Parliament, the Public Service Commission and the rest.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.sgdi.pages.organs_of_state`, which tracks
row `organs_of_state` of `sources/sg-sgdi/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "organs_of_state"
