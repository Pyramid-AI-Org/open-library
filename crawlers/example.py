from __future__ import annotations

from datetime import datetime, timezone

import requests

from crawlers.base import RunContext, UrlRecord


class Crawler:
    name = "example"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        seed_urls: list[str] = (
            ctx.settings.get("crawlers", {}).get(self.name, {}).get("seed_urls", [])
        )
        out: list[UrlRecord] = []

        for seed in seed_urls:
            # Example-only: fetch and emit a single record about the seed
            requests.get(
                seed,
                timeout=int(ctx.settings.get("http", {}).get("timeout_seconds", 30)),
                headers={"User-Agent": ctx.settings.get("http", {}).get("user_agent", "")},
            )
            out.append(
                UrlRecord(
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
