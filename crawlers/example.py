from __future__ import annotations

from datetime import datetime, timezone

import requests

from crawlers.base import RunContext, UrlRecord


class Crawler:
    name = "example"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        seed_urls: list[str] = (
            ctx.get_crawler_config(self.name).get("seed_urls", [])
        )
        out: list[UrlRecord] = []

        for seed in seed_urls:
            # Example-only: fetch and emit a single record about the seed
            requests.get(
                seed,
                timeout=int(ctx.get_http_config().get("timeout_seconds", 30)),
                headers={"User-Agent": ctx.get_http_config().get("user_agent", "")},
            )
            out.append(
                ctx.make_record(
                    url=seed,
                    name=None,
                    discovered_at_utc=datetime.now(timezone.utc).isoformat(),
                    source=self.name,
                    meta={},
                )
            )

        # Deterministic ordering for stable diffs
        out.sort(key=lambda r: r.url)
        return out
