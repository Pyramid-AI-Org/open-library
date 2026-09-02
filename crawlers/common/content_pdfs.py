"""Collect every document linked inside a page's content element.

Most departmental crawlers are written around the markup of the one page they
were built for — a table layout here, `div.col` cards there — and return nothing
when pointed at a sibling page that lists the same kind of documents in a
different shape. That is how the FSD obsolete circulars, the BD common
conditions and several other sections stayed empty: not a fetch failure, a
layout assumption.

This crawler makes no structural assumption beyond "the documents are linked
inside the content element". It is deliberately dumber than the per-department
parsers: no titles from table rows, no publish dates inferred from surrounding
markup. Use it for a listing page whose own department's parser does not fit,
and prefer the specific parser when it does.

Config:
    page_url            single page to read
    page_urls           several pages; takes precedence over page_url
    content_element_id  element to look inside (default "content")
    doc_extensions      extensions to keep (default pdf/doc/docx/xls/xlsx)
    url_prefix          optional prefix every kept document must start with
    exclude_url_patterns  regexes; a document whose URL matches any is skipped
"""

from __future__ import annotations

import random
import re

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element

_DEFAULT_DOC_EXTS = (".pdf", ".doc", ".docx", ".xls", ".xlsx")


class ContentPdfCrawler:
    """Base for sections that are just "the documents on this page".

    Subclasses set `name`; everything else comes from settings.
    """

    name = "content_pdfs"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_urls = [
            str(u).strip() for u in (cfg.get("page_urls") or []) if str(u).strip()
        ]
        if not page_urls:
            single = str(cfg.get("page_url", "")).strip()
            page_urls = [single] if single else []
        if not page_urls:
            raise ValueError(f"{self.name}: neither page_url nor page_urls is set")

        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )
        doc_exts = tuple(
            str(e).strip().lower()
            for e in (cfg.get("doc_extensions") or _DEFAULT_DOC_EXTS)
            if str(e).strip()
        )
        url_prefix = str(cfg.get("url_prefix", "")).strip()
        # Several departments publish a signed scan and a zipped archive of
        # superseded editions alongside each current document. They carry no
        # text the current version does not, so they are excluded by pattern
        # rather than collected and parsed twice.
        exclude_patterns = [
            re.compile(str(pat), re.IGNORECASE)
            for pat in (cfg.get("exclude_url_patterns") or [])
            if str(pat).strip()
        ]

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen: set[str] = set()

        for page_url in page_urls:
            if request_delay_seconds > 0:
                sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            try:
                resp = get_with_retries(
                    session,
                    page_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except Exception as exc:
                # One dead page in a multi-page section should not lose the rest.
                if ctx.debug:
                    print(f"[{self.name}] Skip page fetch failure: {page_url} ({exc})")
                continue

            html = resp.text or ""
            links = extract_links_in_element(
                html, base_url=page_url, element_id=content_element_id
            )
            if not links:
                # Some pages carry no such element; fall back to the whole page
                # rather than silently reporting an empty section.
                links = extract_links(html, base_url=page_url)

            for link in links:
                can = canonicalize_url(link.href, encode_spaces=True)
                if not can:
                    continue
                if path_ext(can) not in doc_exts:
                    continue
                if url_prefix and not can.startswith(url_prefix):
                    continue
                if any(pat.search(can) for pat in exclude_patterns):
                    continue
                if can in seen:
                    continue
                seen.add(can)

                out.append(
                    ctx.make_record(
                        url=can,
                        name=infer_name_from_link(clean_text(link.text or ""), can),
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={"discovered_from": page_url},
                    )
                )
                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or "", r.name or ""))
        return out
