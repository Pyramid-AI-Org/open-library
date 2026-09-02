"""TD free-publication sub-series.

The free-publications index links out to nine sub-series and carries only ten
documents itself. Three of those sub-series were configured; the other six —
environmental reports, ERP public engagement, the Mid-Levels traffic study,
goods-vehicle survey, TCSFR and the annual traffic census — were not, so their
documents were unreachable.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "free_publications_subsections"
