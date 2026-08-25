"""Singapore Civil Defence Force - fire code.

The Fire Code itself - the Code of Practice for Fire Precautions in
Buildings - plus the rapid transit and road tunnel codes. The Hong Kong note
beside the equivalent slide read 'download only the 13th version is okay';
here the current consolidated Fire Code 2023 plays that role.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.scdf.pages.fire_code`, which tracks
row `fire_code` of `sources/sg-scdf/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "fire_code"
