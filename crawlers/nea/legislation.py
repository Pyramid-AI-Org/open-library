"""National Environment Agency - legislation.

The Acts NEA administers - Environmental Protection and Management,
Environmental Public Health, Hazardous Waste, Resource Sustainability,
Transboundary Haze Pollution - and their subsidiary legislation. All the
text lives on Singapore Statutes Online rather than on NEA, so these are
recorded as external references. Dropping them would leave codes of practice
in the library with no route to the law behind them.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nea.pages.legislation`, which tracks
row `legislation` of `sources/sg-nea/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "legislation"
