"""CAAS - obstacle height control and obstacle clearance.

The height limits themselves, and the clearance application a developer files
when a structure may penetrate them. This is the row a project team actually
needs before designing anything tall near Changi, Seletar or Paya Lebar.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.sg.yaml` under
`crawlers.caas.pages.obstacle_height_control`, which tracks row
`obstacle_height_control` of `sources/sg-caas/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "obstacle_height_control"
