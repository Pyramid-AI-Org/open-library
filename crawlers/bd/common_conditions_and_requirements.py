"""BD common conditions and requirements (SE-SA / SE-SC series).

The conditions a structural submission has to satisfy. 266 documents on a single
page, none of them held: the section was never seeded at all, and it is the
largest single-page gap anywhere in the Buildings Department.

Built on the scheduled-areas crawler rather than the codes-and-design-manuals
one. The latter's parser is built around the div-based layout of its own index
and returns nothing here, where the documents sit in table rows. The
scheduled-areas crawler simply takes every PDF inside the content element, which
is all this page needs.

`name` must be overridden: the crawler resolves its settings by that attribute,
so inheriting the parent's name would re-run the parent's section instead.
"""

from __future__ import annotations

from crawlers.bd.scheduled_areas import Crawler as _ContentPdfCrawler


class Crawler(_ContentPdfCrawler):
    name = "common_conditions_and_requirements"
