"""BCA accessibility and universal design crawler.

Covers the Universal Design guides and index material that sit under
/home-and-building-owners/ but are technical design references rather than
consumer content.

Behaviour lives in `crawlers.common.isomer.IsomerSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.bca.pages.accessibility_and_universal_design`, which
tracks row `accessibility_and_universal_design` of `sources/sg-bca/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.isomer import IsomerSectionCrawler


class Crawler(IsomerSectionCrawler):
    name = "accessibility_and_universal_design"
