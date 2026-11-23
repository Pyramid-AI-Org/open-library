from crawlers.common.document_list import DocumentListCrawler


class Crawler(DocumentListCrawler):
    """EMSD ordinances and subsidiary legislation PDFs (PAI-1741)."""

    name = "legislation"
