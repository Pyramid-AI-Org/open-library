"""Urban Redevelopment Authority - master plan.

The Master Plan written statement and its amendments - the statutory land
use plan every development is assessed against.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ura.pages.master_plan`, which tracks
row `master_plan` of `sources/sg-ura/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "master_plan"
