"""Energy Market Authority - forms.

Forms and agreements that are part of a regulatory process. A submission is
judged against the current version of the form, so the form is part of the
requirement.

Behaviour lives in `crawlers.common.payload.ApiIndexCrawler`; scope lives in
`config/settings.yaml` under `crawlers.ema.pages.forms`, which tracks row `forms` of
`sources/sg-ema/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import ApiIndexCrawler


class Crawler(ApiIndexCrawler):
    name = "forms"
