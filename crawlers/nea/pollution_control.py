"""National Environment Agency - pollution control.

The Air, Noise, Water, chemical safety and toxic industrial waste topic
trees. The Hong Kong deck asked for web text here rather than files, and
that instruction is load-bearing on this site: NEA states several binding
limits only as HTML tables - the 36-row trade effluent discharge limits and
the industrial and construction boundary noise limits have no PDF at all. A
document-only crawl would miss the actual standards.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nea.pages.pollution_control`, which tracks
row `pollution_control` of `sources/sg-nea/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "pollution_control"
