"""Labour Department — documents the department's own crawlers do not reach.

Occupational health guides on index pages the department crawlers do not visit.

A sweep section: the pages listed in settings hold documents that the
department's bespoke parsers miss, either because the markup differs from the
page they were written for or because the section was never seeded. Signed
duplicates and zipped archives of superseded editions are excluded by pattern —
they carry no text the current version does not.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "labour_document_sweep"
