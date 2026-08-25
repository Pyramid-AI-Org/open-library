"""LTA transport infrastructure design criteria and specifications crawler.

The core engineering standards for rail and road infrastructure: the Civil
Design Criteria, the Infrastructure and Architectural Design Criteria volumes,
the Materials and Workmanship specifications, and the Standard Details of Road
Elements. The Singapore counterpart of the Highways Department technical
document set already in the library.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives
in `config/settings.yaml` under `crawlers.lta.pages.transport_infrastructure_standards`,
which tracks row `transport_infrastructure_standards` of `sources/sg-lta/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "transport_infrastructure_standards"
