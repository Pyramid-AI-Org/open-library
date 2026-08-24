"""National Environment Agency - practices and guidelines.

The guidance-document core: the Code of Practice on Environmental Health,
the codes for Environmental Control Officers and Coordinators, the waste
collector and vector control codes, and the cross-topic guideline set
covering boundary noise limits for ACMV systems, pollution control studies
and noise mitigation for foodshops and entertainment outlets.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.nea.pages.practices_and_guidelines`, which tracks
row `practices_and_guidelines` of `sources/sg-nea/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "practices_and_guidelines"
