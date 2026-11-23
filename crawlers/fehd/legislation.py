from crawlers.common.document_list import DocumentListCrawler


class Crawler(DocumentListCrawler):
    """Cap 172 and Cap 132 subsidiary legislation PDFs (PAI-1741)."""

    name = "legislation"
