"""BD codes-and-references sections that have no crawler of their own.

The codes-and-references index has nineteen child sections; six had crawlers
(codes and design manuals, practice notes, central data bank, scheduled areas,
notices and reports, common conditions). The rest — modular integrated
construction, PV panels, public open space, legal matters, pre-accepted computer
programs, innovative materials, epidemic prevention, typhoon precautions,
transitional housing — held 585 documents between them with no route in.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "codes_and_references_sections"
