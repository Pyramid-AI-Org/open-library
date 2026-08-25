"""BCA technical guidebooks and publications crawler.

Covers /resources/guidebooks-and-publications/, including the fixed-installation
regulation guidebooks and the sustainable construction series.

Behaviour lives in `crawlers.common.isomer.IsomerSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.bca.pages.guidebooks_and_publications`, which
tracks row `guidebooks_and_publications` of `sources/sg-bca/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.isomer import IsomerSectionCrawler


class Crawler(IsomerSectionCrawler):
    name = "guidebooks_and_publications"
