"""JTC Corporation - development and renovation.

JTC's technical requirements for building on its land: the Urban Design
Requirements, the Space Submission Handbook, the Fitting-out and Renovation
Works Guidebook with its annexes, the usage guidelines for JTC premises and
the plot ratio exemptions. The forms-and-documents page is fully server-
rendered, which makes this the reliable row here.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.jtc.pages.development_and_renovation`, which tracks
row `development_and_renovation` of `sources/sg-jtc/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "development_and_renovation"
