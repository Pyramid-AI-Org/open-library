"""HyD standard drawings.

The drawings live under /technical_references/standard_drawings/, a sibling of
/technical_references/technical_document/. Only the latter was ever seeded, so
none of the 581 drawings across the three sets (highways, lighting, structures)
were being collected — more documents than the whole of the rest of HyD.

The technical-documents crawler is already a configurable spider over a URL
prefix, so this section needs no new parsing. It does need its own `name`:
the crawler looks its settings up by that attribute, so inheriting the parent's
name would silently re-run the parent's section instead of this one.
"""

from __future__ import annotations

from crawlers.hyd.hyd_technical_documents import Crawler as _TechnicalDocumentsCrawler


class Crawler(_TechnicalDocumentsCrawler):
    name = "hyd_standard_drawings"
