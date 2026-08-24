"""Ministry of Manpower - workplace safety and health.

The rest of the WSH tree: pressure vessels and lifting equipment, medical
and hygiene monitoring, occupational disease reporting, risk management, and
the reports and statistics series. The Hong Kong pressure equipment slide
maps here - though note the finding below, because MOM states most of this
as web text rather than as documents.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.mom.pages.workplace_safety_and_health`, which tracks
row `workplace_safety_and_health` of `sources/sg-mom/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "workplace_safety_and_health"
