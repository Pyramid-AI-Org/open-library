"""DSD — documents the department's own crawlers do not reach.

A sweep section over pages found by the homepage-depth discovery crawl. Signed
duplicates and zipped archives of superseded editions are excluded by pattern.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "dsd_document_sweep"
