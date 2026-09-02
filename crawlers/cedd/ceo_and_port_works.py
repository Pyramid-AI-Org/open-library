"""CEDD Civil Engineering Office publications, including the Port Works Design Manual.

These sit under /publications/ceo/, a sibling of the /publications/geo/ branch
that the GEO crawler covers. Scoping to GEO alone missed the Port Works Design
Manual entirely — five parts plus corrigenda, the governing design reference for
marine and waterfront works.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "ceo_and_port_works"
