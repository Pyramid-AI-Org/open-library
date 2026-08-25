"""National Parks Board - CUGE Standards.

The CS series: soil mixtures, turfgrass installation, rooftop greenery design
loads and substrates, biodiversity on roof gardens. The closest thing NParks has
to a technical-document library, and free to download unlike most Singapore
standards.

CUGE runs on Isomer while the parent site runs Sitefinity, so its PDFs sit on
isomer-user-content.by.gov.sg and older ones on skyrisegreenery.nparks.gov.sg.
Both hosts are in the document allowlist. Because cuge.nparks.gov.sg is a
separate origin it cannot be harvested from a page context on the main host - it
needs its own run.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nparks.pages.cuge_standards`, which tracks
row `cuge_standards` of `sources/sg-nparks/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "cuge_standards"
