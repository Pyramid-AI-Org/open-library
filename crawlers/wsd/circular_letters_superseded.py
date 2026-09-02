"""WSD superseded circular letters.

WSD keeps its superseded circulars on a separate page, reachable only from
inside the current-circulars page. 152 documents going back to 1975 that were
never collected — the largest single block on wsd.gov.hk, and more than four
times the 35 current circulars we already hold.

Same page shape as the current circulars, so it reuses that crawler. `name` is
overridden because the crawler resolves its settings by that attribute.
"""

from __future__ import annotations

from crawlers.wsd.circular_letters import Crawler as _CircularLettersCrawler


class Crawler(_CircularLettersCrawler):
    name = "circular_letters_superseded"
