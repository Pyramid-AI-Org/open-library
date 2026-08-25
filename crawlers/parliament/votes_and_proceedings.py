"""Parliament of Singapore - Votes and Proceedings.

The formal record of each sitting's decisions, back to 12 January 2005 - the
nearest Singapore equivalent of the Hong Kong committee minutes. The oldest
entries are .doc rather than .pdf.

Unlike the Order Paper, this page embeds only the first ten records. The rest
come from a Next.js server action, which returns all 619 in one call when the
limit is raised.

The file is nested under `report`; the public URL is
/api/media/<report.id>/<report.filename_download with spaces hyphenated>. An
earlier attempt inferred /api/media/<record id> and produced 619 wrong URLs -
read the sub-object, do not guess a shape.

Behaviour lives in `crawlers.common.payload.ServerActionCrawler`; scope lives
in `config/settings.yaml` under
`crawlers.parliament.pages.votes_and_proceedings`, which tracks row
`votes_and_proceedings` of `sources/sg-parliament/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ServerActionCrawler


class Crawler(ServerActionCrawler):
    name = "votes_and_proceedings"
