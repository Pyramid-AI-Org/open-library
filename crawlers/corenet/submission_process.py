"""CORENET X - the submission process.

The 3-Gateway process, the Direct Submission Process, submission workflows and
the typical submission package, plus the routes for conservation, external
works, Part-ST and infrastructure works.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.sg.yaml` under `crawlers.corenet.pages.submission_process`,
which tracks row `submission_process` of `sources/sg-corenet/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "submission_process"
