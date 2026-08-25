"""BCA Safety and Standards crawler.

Covers the whole /safety-and-standards/ tree: the Building Control Act page
(codes, acts and regulations), accessibility codes, construction quality,
plan-submission guidelines, periodic building inspections, lifts and
escalators, amusement rides and civil defence shelters.

Behaviour lives in `crawlers.common.isomer.IsomerSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.bca.pages.safety_and_standards`, which
tracks row `safety_and_standards` of `sources/sg-bca/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.isomer import IsomerSectionCrawler


class Crawler(IsomerSectionCrawler):
    name = "safety_and_standards"
