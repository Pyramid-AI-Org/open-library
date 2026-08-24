"""National Parks Board - publications resources.

Includes the Guidelines on Greenery Provision and Tree Conservation for
Developments - NParks acting as a development control authority, and the row a
building professional actually needs.

111 pages for 2 documents looks like a failure and is not. The rest of this
section is web articles with no attached files; verified on a 25-page sample,
raw HTML and DOM agree. Do not widen this row to "fix" the ratio.

Driven from the sitemap at /sitemap/sitemap.xml rather than by following links.
NParks is a Sitefinity site whose *pages* are server-rendered but whose
*listings* are not, so a seed-and-follow crawl sees the seed and nothing under
it - which is what held the first harvest to 119 records.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.nparks.pages.publications_resources`, which tracks row
`publications_resources` of `sources/sg-nparks/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "publications_resources"
