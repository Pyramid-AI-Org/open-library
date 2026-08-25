"""Singapore Civil Defence Force - acts codes and regulations.

The single highest-yield page on the site and the direct answer to the Hong
Kong requirement: the Fire Safety Act and its subsidiary legislation, the
codes of practice, and the associated forms, all as downloadable files on
one server-rendered page.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.scdf.pages.acts_codes_and_regulations`, which tracks
row `acts_codes_and_regulations` of `sources/sg-scdf/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "acts_codes_and_regulations"
