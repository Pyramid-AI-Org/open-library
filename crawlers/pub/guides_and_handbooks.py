"""PUB, Singapore's National Water Agency - guides and handbooks.

The direct analogue of the DSD technical manuals and guidelines. It carries
the Drainage Handbook (Managing Urban Runoff), the ABC Waters Design
Guidelines and their engineering procedures, the Flood-Resilient
Developments Guidebook, the Earth Control Measures guidebook, the Sewer CCTV
guidebook and the Handbook on Application for Water Supply. It is also the
one high-value PUB listing that is fully server-rendered, so it is the most
reliable row in this source.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.pub.pages.guides_and_handbooks`, which tracks
row `guides_and_handbooks` of `sources/sg-pub/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "guides_and_handbooks"
