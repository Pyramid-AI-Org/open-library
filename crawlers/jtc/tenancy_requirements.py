"""JTC Corporation - tenancy requirements.

Change-of-use rules and the solar deployment requirements - technical
conditions on how JTC premises may be used and altered.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.jtc.pages.tenancy_requirements`, which tracks
row `tenancy_requirements` of `sources/sg-jtc/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "tenancy_requirements"
