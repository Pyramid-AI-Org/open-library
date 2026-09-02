"""EMSD — documents the department's own crawlers do not reach.

Electricity, gas, lift and BEEO publications spread across sub-pages and the bestpractice subdomain.

A sweep section: the pages listed in settings hold documents that the
department's bespoke parsers miss, either because the markup differs from the
page they were written for or because the section was never seeded. Signed
duplicates and zipped archives of superseded editions are excluded by pattern —
they carry no text the current version does not.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "emsd_document_sweep"
