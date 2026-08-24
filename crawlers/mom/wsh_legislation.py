"""Ministry of Manpower - wsh legislation.

The Workplace Safety and Health Act and its ~45 subsidiary regulations -
construction, work at heights, confined spaces, noise, scaffolds. All the
text is on Singapore Statutes Online rather than on MOM, so these are
external references. They are the law the codes of practice are written
under and belong in the library alongside them.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.mom.pages.wsh_legislation`, which tracks
row `wsh_legislation` of `sources/sg-mom/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "wsh_legislation"
