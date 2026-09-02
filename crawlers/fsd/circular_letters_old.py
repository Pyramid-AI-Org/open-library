"""FSD old circular letters on ventilating systems (1968-1998).

The third and oldest of the FSD circular pages, again on its own URL.
"""

from __future__ import annotations

from crawlers.fsd.circular_letters import Crawler as _CircularLettersCrawler


class Crawler(_CircularLettersCrawler):
    name = "circular_letters_old"
