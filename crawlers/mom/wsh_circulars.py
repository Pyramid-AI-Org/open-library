"""Ministry of Manpower - wsh circulars.

The best PDF target on MOM: workplace safety and health circulars from 1999
to the present, grouped by year but with every link present in the served
HTML. This is the direct equivalent of the Hong Kong occupational safety and
health document sets.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.mom.pages.wsh_circulars`, which tracks
row `wsh_circulars` of `sources/sg-mom/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "wsh_circulars"
