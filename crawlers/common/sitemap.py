"""Platform-neutral names for the sitemap-driven engines.

The sitemap harvester and the collection-listing harvester were written for
Isomer, which is where they were first needed, and they still live in
``crawlers/common/isomer.py``. Neither is actually Isomer-specific: the
harvester wants a sitemap, a content container and a document rule, and every
one of those is configuration.

Since then they have been pointed at Sitecore, Sitefinity and Adobe AEM sites
with nothing changed but YAML. Importing ``IsomerSectionCrawler`` to crawl the
Ministry of Manpower reads like a mistake, so this module offers the honest
names. It is an alias, not a fork - one implementation, two vocabularies.

    from crawlers.common.sitemap import SitemapSectionCrawler

    class Crawler(SitemapSectionCrawler):
        name = "wsh_circulars"

Choosing between the engines is a Phase 1 question with one input: does the
site publish a usable sitemap?

* It does - ``SitemapSectionCrawler``. The page set is known before anything
  is fetched, so coverage is a count rather than an estimate.
* It does not - ``SectionSpiderCrawler`` in ``crawlers/common/spider.py``,
  bounded by a path allowlist, a depth limit and a page budget.

A sitemap that exists is not automatically usable. Check two things before
relying on it. NEA stamps all ~520 entries with the same 2018 date, so its
``lastmod`` carries no information; and most Sitecore and Sitefinity sitemaps
list HTML pages only, never the documents, so the sitemap discovers *pages*
and the documents still have to be harvested from them.
"""

from __future__ import annotations

from crawlers.common.isomer import (  # noqa: F401
    IsomerCollectionCrawler as CollectionListingCrawler,
    IsomerSectionCrawler as SitemapSectionCrawler,
    assign_titles,
    extract_collection_items,
    extract_page_links,
    extract_page_title,
    is_weak_label,
    parse_sitemap,
    split_label,
    strip_query,
)

__all__ = [
    "SitemapSectionCrawler",
    "CollectionListingCrawler",
    "assign_titles",
    "extract_collection_items",
    "extract_page_links",
    "extract_page_title",
    "is_weak_label",
    "parse_sitemap",
    "split_label",
    "strip_query",
]
