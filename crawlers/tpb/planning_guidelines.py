"""Town Planning Board planning guidelines (TPB PG-No series).

The guidelines the Board applies when deciding planning applications — siting of
GIC facilities, parking provision, industrial-office use, village-type
development. A complete series that the library had no crawler for at all.

TPB is a statutory body with its own site, so this is a new source rather than
part of the Planning Department's.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "planning_guidelines"
