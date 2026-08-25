"""National Environment Agency - development control.

Singapore's nearest thing to a statutory environmental impact regime: the
Pollution Control Study guidelines, air dispersion modelling guidelines, the
Quantitative Risk Assessment criteria and technical guidance, and the land
traffic noise impact assessment guideline. This is the row that answers the
Hong Kong deck's EA & Planning section.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nea.pages.development_control`, which tracks
row `development_control` of `sources/sg-nea/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "development_control"
