"""PUB, Singapore's National Water Agency - circulars.

Industry circulars - the running record of how PUB's requirements change,
and the equivalent of the DSD technical circulars and the WSD plumbing
circulars. Categories include Licensed Plumbers, Building Plans, Earth
Control Measures, Fittings and Standards, Used Water Management and Water
Storage Tanks.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.pub.pages.circulars`, which tracks
row `circulars` of `sources/sg-pub/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "circulars"
