"""WSH Council - advisories and alerts.

WSH Alerts and Advisories - incident-driven notices naming the failure mode and the control expected. Read alongside the codes, they are how a safety requirement gets explained.

The catalogue comes from the Council's own JSON index in one request, and each
entry's detail page holds the PDF. Behaviour lives in
`crawlers.common.json_index.JsonIndexSectionCrawler`; scope lives in
`config/settings.sg.yaml` under `crawlers.wshc.pages.advisories_and_alerts`, which tracks
row `advisories_and_alerts` of `sources/sg-wshc/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.json_index import JsonIndexSectionCrawler


class Crawler(JsonIndexSectionCrawler):
    name = "advisories_and_alerts"
