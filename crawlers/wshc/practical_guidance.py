"""WSH Council - practical guidance.

Checklists, articles, case studies and resource kits - the applied layer beneath the codes.

The catalogue comes from the Council's own JSON index in one request, and each
entry's detail page holds the PDF. Behaviour lives in
`crawlers.common.json_index.JsonIndexSectionCrawler`; scope lives in
`config/settings.sg.yaml` under `crawlers.wshc.pages.practical_guidance`, which tracks
row `practical_guidance` of `sources/sg-wshc/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.json_index import JsonIndexSectionCrawler


class Crawler(JsonIndexSectionCrawler):
    name = "practical_guidance"
