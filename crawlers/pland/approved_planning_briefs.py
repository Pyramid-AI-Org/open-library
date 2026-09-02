"""PlanD approved planning briefs.

Site-specific development parameters approved by the Board — plot ratio, height
limits, GFA concessions — for Housing Department sites and others. 117 documents
under /resources/approved_pb/, a branch the library never seeded.
"""

from __future__ import annotations

from crawlers.common.content_pdfs import ContentPdfCrawler


class Crawler(ContentPdfCrawler):
    name = "approved_planning_briefs"
