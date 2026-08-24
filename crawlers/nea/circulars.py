"""National Environment Agency - circulars.

Four circular series - Development Control, Hazardous Substance Control,
Radiation Protection, and Climate Change & Energy Efficiency - reaching back
to 1995. Each is a plain server-rendered table of title, PDF, size and date,
which makes this the easiest high-yield row on the site.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nea.pages.circulars`, which tracks
row `circulars` of `sources/sg-nea/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "circulars"
