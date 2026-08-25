"""National Parks Board - nature.

Biodiversity guidelines and resources, including the Handbook on Habitat
Restoration and its per-chapter volumes.

Driven from the sitemap at /sitemap/sitemap.xml rather than by following links.
NParks is a Sitefinity site whose *pages* are server-rendered but whose
*listings* are not, so a seed-and-follow crawl sees the seed and nothing under
it - which is what held the first harvest to 119 records.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.nparks.pages.nature`, which tracks row
`nature` of `sources/sg-nparks/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "nature"
