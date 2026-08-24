"""Parliament of Singapore - Bills Introduced.

Every Bill as introduced, back to January 2006. For a library whose subject is
regulation this is the most valuable of the three Parliament rows: it is where
changes to the Acts the codes are written under first appear.

Same server-action shape as votes_and_proceedings with its own field names. Each
record carries the Bill number in `title`, the Bill name in `description`, a
`file` object, and sometimes a `corrigenda` object - both files are taken.

Behaviour lives in `crawlers.common.payload.ServerActionCrawler`; scope lives
in `config/settings.yaml` under `crawlers.parliament.pages.bills_introduced`,
which tracks row `bills_introduced` of `sources/sg-parliament/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ServerActionCrawler


class Crawler(ServerActionCrawler):
    name = "bills_introduced"
