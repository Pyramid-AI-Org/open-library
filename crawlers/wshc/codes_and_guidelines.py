"""WSH Council - codes and guidelines.

Approved Codes of Practice, WSH Guidelines, Guides and Handbooks and technical advisories - the standards a site is actually held to, and the direct counterpart to the Hong Kong occupational safety sets.

The catalogue comes from the Council's own JSON index in one request, and each
entry's detail page holds the PDF. Behaviour lives in
`crawlers.common.json_index.JsonIndexSectionCrawler`; scope lives in
`config/settings.sg.yaml` under `crawlers.wshc.pages.codes_and_guidelines`, which tracks
row `codes_and_guidelines` of `sources/sg-wshc/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.json_index import JsonIndexSectionCrawler


class Crawler(JsonIndexSectionCrawler):
    name = "codes_and_guidelines"
