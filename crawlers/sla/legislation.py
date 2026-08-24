"""Singapore Land Authority - legislation.

The 13 Acts SLA administers - State Lands, State Lands Protection, Land
Surveyors, Boundaries and Survey Maps, Land Titles. All on Statutes Online,
recorded as external references.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives in
`config/settings.yaml` under `crawlers.sla.pages.legislation`, which tracks
row `legislation` of `sources/sg-sla/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "legislation"
