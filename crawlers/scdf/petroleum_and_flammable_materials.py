"""Singapore Civil Defence Force - petroleum and flammable materials.

Petroleum and flammable materials licensing, storage requirements and the
transport circulars for dangerous goods - the hazardous-materials half of
SCDF's regulatory remit.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.scdf.pages.petroleum_and_flammable_materials`, which tracks
row `petroleum_and_flammable_materials` of `sources/sg-scdf/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "petroleum_and_flammable_materials"
