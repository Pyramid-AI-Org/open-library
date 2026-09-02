"""CEDD Mines Division guidance notes and practice notes.

Blasting, explosives handling and quarry guidance. These live under
/our-major-services/explosives-blasting-quarries/ rather than /publications/,
so a crawler scoped to the publications tree never reaches them.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "mines_and_blasting"
