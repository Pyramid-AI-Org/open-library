"""National Parks Board - who we are.

Corporate pages, kept only for the small number of statutory documents filed
under them.

Driven from the sitemap at /sitemap/sitemap.xml rather than by following links.
NParks is a Sitefinity site whose *pages* are server-rendered but whose
*listings* are not, so a seed-and-follow crawl sees the seed and nothing under
it - which is what held the first harvest to 119 records.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.nparks.pages.who_we_are`, which tracks row
`who_we_are` of `sources/sg-nparks/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "who_we_are"
