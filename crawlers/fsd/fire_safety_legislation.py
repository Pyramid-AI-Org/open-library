from crawlers.common.document_list import DocumentListCrawler


class Crawler(DocumentListCrawler):
    """Caps 502, 572 and 636 fire safety ordinance PDFs, shared with BD (PAI-1741)."""

    name = "fire_safety_legislation"
