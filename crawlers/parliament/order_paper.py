"""Parliament of Singapore - Order Paper.

The agenda of each sitting: what Parliament was asked to consider and when.
2,168 items running back to 22 April 1955 - the deepest single run of primary
material in the Singapore set.

This row needs no pagination. The first response embeds the whole collection in
its Next.js flight payload under `initData`, and each item's `fileUrl` is
already the correct /api/media/ path.

The 21 August 2026 correction is worth remembering: an earlier version of this
crawler walked 40 listing pages and returned zero documents, because the item
links are React handlers rather than anchors and the configured document prefix
(`/docs/default-source/`) excluded every file. Nothing was wrong with the site.

Behaviour lives in `crawlers.common.payload.FlightPayloadCrawler`; scope lives
in `config/settings.yaml` under `crawlers.parliament.pages.order_paper`, which
tracks row `order_paper` of `sources/sg-parliament/scope.yaml`.
"""

from __future__ import annotations

from crawlers.common.payload import FlightPayloadCrawler


class Crawler(FlightPayloadCrawler):
    name = "order_paper"
