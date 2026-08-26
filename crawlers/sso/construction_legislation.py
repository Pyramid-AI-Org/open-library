"""Singapore Statutes Online - the Acts construction is governed by.

Every code, circular and guideline the library already holds is written under
one of these Acts, and until now they were recorded only as external references
when another agency happened to link to one. This row collects the law itself.

Deliberately a named list rather than a crawl of the statute book. Two reasons.
The statute book is thousands of instruments and almost none of it concerns how
a building is built, so a prefix crawl would trade precision for volume. And
Statutes Online rate-limits firmly - a path that answers 200 will answer 403
minutes later under load - so the polite shape here is a short list fetched
slowly, not a broad walk.

Subsidiary legislation is not collected yet. That is a scope decision recorded
in sources/sg-sso/scope.yaml, not an oversight: each Act carries its own
regulations, and pulling them in multiplies the request count against a source
that has already shown it will push back.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.sg.yaml` under
`crawlers.sso.pages.construction_legislation`, which tracks row
`construction_legislation` of `sources/sg-sso/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "construction_legislation"
