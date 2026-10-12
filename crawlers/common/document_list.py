"""Register a bounded list of official documents in the normal crawler feed.

Unlike an HTML sweep, this preserves the reviewed titles of direct PDF links.
The file processor downloads them; the daily orchestrator owns change checks.
Unknown publication dates stay null rather than being inferred from editions.
"""

from urllib.parse import urlparse

from crawlers.base import RunContext, UrlRecord, canonicalize_url, path_ext


class DocumentListCrawler:
    name = "licensing_guides"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        documents = cfg.get("documents") or []
        if not documents:
            raise ValueError(f"{ctx.source_id}.{self.name}: no documents configured")
        hosts = set(cfg.get("document_hosts") or [])
        records: dict[str, UrlRecord] = {}
        for document in documents:
            url = canonicalize_url(document["url"], encode_spaces=True)
            title = str(document.get("title") or "").strip()
            if not url or path_ext(url) != ".pdf" or not title:
                raise ValueError(f"{ctx.source_id}.{self.name}: invalid PDF entry")
            if urlparse(url).scheme != "https" or urlparse(url).hostname not in hosts:
                raise ValueError(f"{ctx.source_id}.{self.name}: unexpected document host")
            records[url] = ctx.make_record(
                url=url,
                name=title,
                source=f"{ctx.source_id}.{self.name}",
                discovered_at_utc=ctx.started_at_utc,
                publish_date=document.get("publish_date"),
                meta={"file_ext": "pdf", "record_kind": "document", "ticket": "PAI-1457"},
            )
        return [records[url] for url in sorted(records)]
