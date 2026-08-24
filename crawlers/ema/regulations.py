"""Energy Market Authority - regulations.

The Electricity Act, the Gas Act, the District Cooling Act and their subsidiary
regulations. Server-rendered, and almost every link leaves for Singapore
Statutes Online, so these are external references rather than documents.

The binding technical standard for electrical installations is Singapore
Standard SS 638, sold by Enterprise Singapore. EMA names it but does not publish
it; SS 650, TR 77 and TR 25 are the same. None can be collected.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.regulations`, which tracks row `regulations` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "regulations"
