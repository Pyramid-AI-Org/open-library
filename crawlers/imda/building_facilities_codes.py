"""IMDA - codes of practice for facilities in buildings.

COPIF, the Code of Practice for Info-communication Facilities in Buildings, and
the internal telecom wiring code that sits with it. Both are mandatory design
input: a development has to provide the risers, ducts, spaces and lead-in
pipes these codes specify, and the requirements also appear in land-use
proposals submitted to URA.

Only this subset of IMDA is in scope. The rest of the authority regulates
broadcasting, telecoms competition and online safety, which are nothing to do
with how a building is built.

The documents are named only inside the Next.js payload - twenty PDFs under
/assets/<uuid>.pdf, invisible to a DOM reader - so this row runs with
`read_payload_documents`.

Behaviour lives in `crawlers.common.sitemap.SitemapSectionCrawler`; scope lives
in `config/settings.sg.yaml` under
`crawlers.imda.pages.building_facilities_codes`, which tracks row
`building_facilities_codes` of `sources/sg-imda/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.sitemap import SitemapSectionCrawler


class Crawler(SitemapSectionCrawler):
    name = "building_facilities_codes"
