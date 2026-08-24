"""Singapore Civil Defence Force - fire safety circulars.

Circular letters - amendments to the Fire Code and how it is to be applied.
Without them the code is a snapshot with no amendment history, which is the
same argument that put the BCA circulars in scope.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.scdf.pages.fire_safety_circulars`, which tracks
row `fire_safety_circulars` of `sources/sg-scdf/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "fire_safety_circulars"
