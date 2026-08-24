"""PUB, Singapore's National Water Agency - requirements.

The WSD half of this source. Water supply services, water fittings, water
storage tanks, licensed plumbers and qualified-person submissions. The Hong
Kong deck asked for the web text of these pages as well as the PDFs, and
that matters more here than it did there: PUB states most plumbing
requirements as page text rather than as documents, so a PDF-only crawl
would return almost nothing from a section that is genuinely substantive.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.pub.pages.requirements`, which tracks
row `requirements` of `sources/sg-pub/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "requirements"
