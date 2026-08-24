"""Bounded section spider for government sites with no usable sitemap.

The Isomer engine (``crawlers/common/isomer.py``) can start from a sitemap and
know the page set before it fetches anything. Plenty of government sites offer
no such thing - Adobe AEM sites like LTA's, most hand-rolled static sites, and
anything behind a CMS that never learned to publish one. For those, the page
set has to be discovered by following links, and the whole risk is that a
spider wanders off into the rest of the domain.

This engine is built around that risk. Three independent bounds apply at once:

* **A path allowlist.** Only URLs under ``allowed_path_prefixes`` are ever
  followed. Navigation, footers and mega-menus point outside the section, so
  they are dropped without needing to identify them as navigation.
* **A depth limit.** Counted from each start URL, so a section that turns out
  to be deeper than expected stops rather than expands.
* **A page budget.** A hard cap on fetches, reported when reached rather than
  passed over in silence - a crawl that quietly stopped early looks exactly
  like a section that was fully covered.

Documents are separated from pages by their own rules rather than by
extension alone. On AEM every asset lives under ``/content/dam/``, and nothing
in the navigation points there, which makes ``document_path_prefixes`` a
sharper filter than any content-container heuristic.

This mirrors the shape of the existing ``crawlers/archsd/*`` and
``crawlers/hyd/hyd_technical_documents`` crawlers, generalised so a new
section is a config block rather than another copy of the loop.

Only ``requests`` and the standard library are used.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import random
import re
from typing import Any
from urllib.parse import urlparse, urlunparse

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
from crawlers.common.isomer import (
    assign_titles,
    extract_page_links,
    extract_page_title,
)


DEFAULT_DOCUMENT_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".zip",
    ".dwg",
)

# Page extensions worth following. A URL with any other extension is an asset,
# not a page, and following it would download a file to parse as HTML.
_PAGE_EXTS: tuple[str, ...] = ("", ".html", ".htm", ".aspx", ".jsp", ".php")


def _as_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return []


def _canon(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _strip_query(url: str) -> str:
    """Drop the query string for identity purposes.

    Government CMSes hang cache-busting and tracking parameters off asset URLs
    (``?sfvrsn=...`` on CORENET, for instance). Left in place they make the
    same document look new on every run, which turns the daily archive diff
    into noise. The fetchable URL is kept in metadata.
    """
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


@dataclass
class _Budget:
    max_pages: int
    fetched: int = 0
    truncated: bool = False

    def take(self) -> bool:
        if self.fetched >= self.max_pages:
            self.truncated = True
            return False
        self.fetched += 1
        return True


class SectionSpiderCrawler:
    """Crawl a bounded section of a site and record the documents in it.

    A subclass supplies only ``name``; the section, its bounds and what counts
    as a document all come from ``config/settings.yaml``, so that adding a
    section is a config change rather than a code change.

    Recognised config keys
    ----------------------
    start_urls                Where to begin. Usually the section landing page.
    allowed_path_prefixes     Only page URLs under these are followed. This is
                              the scope decision and mirrors one row of the
                              approved scope spec.
    exclude_path_patterns     Regexes applied to the path; matches are neither
                              followed nor recorded.
    allowed_hosts             Hosts whose pages may be followed. Defaults to
                              the hosts of the start URLs.
    document_hosts            Hosts whose files count as this agency's output.
    document_path_prefixes    Path prefixes that mark an asset store, e.g.
                              ``/content/dam/``. When set, a file must sit
                              under one of them to be recorded - much sharper
                              than filtering by extension alone.
    external_document_hosts   Hosts that are not the agency but hold primary
                              material - a legislation register, or a shared
                              government circulars portal. Recorded with
                              meta.record_kind = "external_reference".
    document_extensions       File extensions treated as documents.
    emit_page_records         Record the pages themselves, not only the files
                              they link to. Set this when the scope spec asks
                              for a page's web-text.
    strip_document_query      Drop query strings from document identity.
                              Default true; see _strip_query.
    max_depth                 Link hops from a start URL. Default 4.
    max_pages                 Hard cap on pages fetched. Default 400.
    max_out_links_per_page    Guard against a single page with thousands of
                              links. Default 800.
    content_element_id        Optional element id to scope extraction to. Leave
                              unset where the path allowlist already does the
                              work.
    """

    name = "section_spider"

    # -- config plumbing ----------------------------------------------------

    def _session(self, ctx: RunContext) -> requests.Session:
        session = requests.Session()
        user_agent = str(ctx.get_http_config().get("user_agent", "")).strip()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})
        return session

    def _fetch(
        self,
        session: requests.Session,
        url: str,
        *,
        ctx: RunContext,
        cfg: dict[str, Any],
    ) -> str | None:
        delay = float(cfg.get("request_delay_seconds", 0.5))
        jitter = float(cfg.get("request_jitter_seconds", 0.25))
        if delay > 0:
            sleep_seconds(delay + random.uniform(0.0, jitter))
        try:
            resp = get_with_retries(
                session,
                url,
                timeout_seconds=int(ctx.get_http_config().get("timeout_seconds", 30)),
                max_retries=int(ctx.get_http_config().get("max_retries", 3)),
                backoff_base_seconds=float(cfg.get("backoff_base_seconds", 0.5)),
                backoff_jitter_seconds=float(cfg.get("backoff_jitter_seconds", 0.25)),
            )
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if content_type and "html" not in content_type:
                return None  # an asset served from a page-shaped URL
            return resp.text or ""
        except Exception as exc:
            if ctx.debug:
                print(f"[{self.name}] error fetching {url}: {exc}")
            return None

    # -- crawl --------------------------------------------------------------

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        start_urls = _as_list(cfg.get("start_urls")) or _as_list(cfg.get("start_url"))
        if not start_urls:
            return []

        prefixes = _as_list(cfg.get("allowed_path_prefixes"))
        excludes = [re.compile(p) for p in _as_list(cfg.get("exclude_path_patterns"))]
        allowed_hosts = {h.lower() for h in _as_list(cfg.get("allowed_hosts"))}
        if not allowed_hosts:
            allowed_hosts = {urlparse(u).netloc.lower() for u in start_urls}

        doc_hosts = {h.lower() for h in _as_list(cfg.get("document_hosts"))} or set(allowed_hosts)
        doc_prefixes = _as_list(cfg.get("document_path_prefixes"))
        ext_hosts = {h.lower() for h in _as_list(cfg.get("external_document_hosts"))}
        doc_exts = {
            e.lower() if e.startswith(".") else f".{e.lower()}"
            for e in (_as_list(cfg.get("document_extensions")) or DEFAULT_DOCUMENT_EXTENSIONS)
        }

        emit_pages = bool(cfg.get("emit_page_records", True))
        strip_query = bool(cfg.get("strip_document_query", True))
        content_element_id = str(cfg.get("content_element_id", "") or "").strip() or None
        max_depth = int(cfg.get("max_depth", 4))
        max_out_links = int(cfg.get("max_out_links_per_page", 800))
        max_records = int(cfg.get("max_total_records", 50000))
        budget = _Budget(max_pages=int(cfg.get("max_pages", 400)))

        def in_section(url: str) -> bool:
            parsed = urlparse(url)
            if parsed.netloc.lower() not in allowed_hosts:
                return False
            path = parsed.path or "/"
            if prefixes and not any(path.startswith(p) for p in prefixes):
                return False
            return not any(pattern.search(path) for pattern in excludes)

        def is_document(url: str) -> bool:
            parsed = urlparse(url)
            host, path = parsed.netloc.lower(), (parsed.path or "").lower()
            if host not in doc_hosts:
                return False
            if doc_prefixes and not any(path.startswith(p.lower()) for p in doc_prefixes):
                return False
            return path_ext(url) in doc_exts

        session = self._session(ctx)
        out: list[UrlRecord] = []
        recorded: set[str] = set()
        visited: set[str] = set()

        queue: deque[tuple[str, int]] = deque()
        for url in start_urls:
            canon = _canon(url)
            if canon and canon not in visited:
                visited.add(canon)
                queue.append((canon, 0))

        while queue and len(out) < max_records:
            page_url, depth = queue.popleft()
            if not budget.take():
                break

            html = self._fetch(session, page_url, ctx=ctx, cfg=cfg)
            if html is None:
                continue

            page_title = extract_page_title(html)

            if emit_pages and page_url not in recorded:
                recorded.add(page_url)
                out.append(
                    ctx.make_record(
                        url=page_url,
                        name=page_title or infer_name_from_link(None, page_url),
                        discovered_at_utc=ctx.started_at_utc,
                        source=f"{ctx.source_id}.{self.name}",
                        publish_date=None,
                        meta={
                            "record_kind": "page",
                            "file_ext": "html",
                            "crawl_depth": depth,
                        },
                    )
                )

            links = extract_page_links(
                html, base_url=page_url, element_id=content_element_id
            )[:max_out_links]

            for link, title, hints in assign_titles(links):
                if len(out) >= max_records:
                    break

                canon = _canon(link.href)
                if not canon:
                    continue

                host = urlparse(canon).netloc.lower()
                doc = is_document(canon)
                # Host membership is the decision; extension is not part of
                # it. A regulator's circular published as a web page on a
                # shared portal is still that regulator's circular.
                external = (not doc) and host in ext_hosts

                if doc or external:
                    identity = _strip_query(canon) if strip_query else canon
                    if identity in recorded:
                        continue
                    recorded.add(identity)

                    meta: dict[str, Any] = {
                        "record_kind": "document" if doc else "external_reference",
                        "discovered_from": page_url,
                        "discovered_from_title": page_title,
                        "file_ext": path_ext(identity).lstrip("."),
                        "crawl_depth": depth,
                    }
                    if identity != canon:
                        meta["fetch_url"] = canon
                    meta.update(hints)

                    out.append(
                        ctx.make_record(
                            url=identity,
                            name=title,
                            discovered_at_utc=ctx.started_at_utc,
                            source=f"{ctx.source_id}.{self.name}",
                            publish_date=None,
                            meta=meta,
                        )
                    )
                    continue

                # Otherwise: is it a page worth following?
                if depth >= max_depth or canon in visited:
                    continue
                if path_ext(canon) not in _PAGE_EXTS or not in_section(canon):
                    continue
                visited.add(canon)
                queue.append((canon, depth + 1))

        if budget.truncated:
            # Saying this out loud matters: a crawl that stopped at its cap and
            # said nothing is indistinguishable from one that covered the
            # section, and the coverage report would score it as a pass.
            print(
                f"[{self.name}] page budget of {budget.max_pages} reached with "
                f"{len(queue)} url(s) still queued - raise max_pages or narrow "
                "allowed_path_prefixes; this run is INCOMPLETE"
            )

        out.sort(key=lambda r: (r.url or ""))
        return out
