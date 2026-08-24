"""BCA environmental sustainability legislation crawler.

Covers the Building Control (Environmental Sustainability) regime: the codes
for new and existing buildings, the Mandatory Energy Improvement code and the
periodic energy audit code. Regulatory rather than promotional - the Green Mark
scheme pages are deliberately out of scope.

Behaviour lives in `crawlers.common.isomer.IsomerSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.bca.pages.sustainability_legislation`, which
tracks row `sustainability_legislation` of `sources/sg-bca/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.isomer import IsomerSectionCrawler


class Crawler(IsomerSectionCrawler):
    name = "sustainability_legislation"
