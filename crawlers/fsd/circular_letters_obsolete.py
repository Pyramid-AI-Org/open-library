"""FSD obsolete circular letters (1987-2021).

A separate page from the current circulars, and one that was not on our radar at
all. Kept because a building approved under an obsolete circular was approved
under the rules of its day, so the superseded text is what explains it.

The FSD circular parser reads `div.col` cards and finds only one link here,
where the documents sit in a different layout — so this section uses the generic
content collector instead.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "circular_letters_obsolete"
