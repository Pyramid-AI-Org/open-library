"""CORENET X - Code of Practice.

The CORENET X Code of Practice and its edition-by-edition summaries of changes.
This is the document that says how a regulatory submission must be prepared and
what the model has to contain, so it governs the form of nearly every other
submission the library already tracks.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.sg.yaml` under `crawlers.corenet.pages.code_of_practice`, which
tracks row `code_of_practice` of `sources/sg-corenet/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "code_of_practice"
