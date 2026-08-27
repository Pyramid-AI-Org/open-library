"""CORENET X - resources and circulars.

Circulars, training material and project references. Note the circulars listing
renders client-side, so a page-following crawl reaches the circulars that are
linked from content pages and not necessarily the full listing; the scope row
records that limit rather than implying complete coverage.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.sg.yaml` under `crawlers.corenet.pages.resources`, which tracks
row `resources` of `sources/sg-corenet/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "resources"
