"""Shared crawling engine for Isomer / Isomer-Next government websites.

Isomer is the static-site platform run by Open Government Products; a large
share of Singapore government agency sites are built on it (BCA, and many
others). Because every Isomer site shares the same skeleton, one engine can
serve every agency and each per-agency crawler module shrinks to a few lines
plus a config block.

Three facts about Isomer-Next sites make this possible:

1. Every site publishes a complete, accurate ``/sitemap.xml`` with ``lastmod``
   timestamps. There is no need to spider link-by-link and risk missing a page
   that is only reachable through a mega-menu, and no need to guess how deep
   to recurse. Enumerate the sitemap, keep the paths under the sections in
   scope, and the page set is settled.

2. Content pages are server-rendered with a ``<main id="main-content">``
   wrapper. Scoping link extraction to that element drops the header, footer,
   breadcrumb and social links without any per-page tuning. Document links
   carry an ``aria-label`` that holds the full human title plus the file type
   and size (``"Approved Document [PDF, 5.1 MB] (opens in new tab)"``), which
   is a far better record name than the visible anchor text.

3. Listing pages ("collections" such as Circulars) render only the first page
   of results as HTML, but the **entire** collection is present in the Next.js
   flight payload embedded in the same response. Reading that payload yields
   every item with its title and publication date in a single request, with no
   pagination to walk and no JavaScript to execute.

Only ``requests`` and the standard library are used, matching the rest of the
repository.
"""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import json
import random
import re
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse, urlunparse
from xml.etree import ElementTree

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


DEFAULT_DOCUMENT_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".zip",
)

# Anchors whose label is decoration rather than a document title.
_LABEL_NOISE_RE = re.compile(r"\s*\(opens in new tab\)\s*$", re.IGNORECASE)
# Isomer appends "[PDF, 5.1 MB]" to aria-labels. Worth keeping as metadata,
# but it does not belong in the record name. The hint is not always at the end:
# in card layouts the anchor wraps the title, the hint, and then a paragraph of
# description, so the hint marks where the title stops.
_FILE_HINT_RE = re.compile(r"\s*\[(?P<kind>[A-Za-z]+),\s*(?P<size>[^\]]+)\]")
# The same hint with its opening bracket lost to markup, e.g. "... PDF, 81 KB]".
_ORPHAN_HINT_RE = re.compile(
    r"\s*(?P<kind>PDF|DOC|DOCX|XLS|XLSX|PPT|PPTX|ZIP),\s*(?P<size>[\d.]+\s*[KMG]B)\]?\s*$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


# Labels that identify nothing on their own. A document called "Download" is
# unfindable later, and the record name is what a person searches on.
_WEAK_LABEL_RE = re.compile(
    r"^(?:download|click here|here|this|view|see|more|read more|link|pdf|"
    r"official)?\s*(?:circular|guide|guidelines?|advisory|document|form|"
    r"notice|example|info(?:rmation)?|details?|website|page|list|report|"
    r"here|more info)?[\s.,:;]*$",
    re.IGNORECASE,
)
_LEAD_IN_RE = re.compile(
    r"^(?:refer to|click|see|read|download|view|for more|please refer)\b",
    re.IGNORECASE,
)


def is_weak_label(text: str) -> bool:
    """True when a label would not help someone find this document later."""
    value = clean_text(text)
    if len(value) < 5:
        return True
    if _WEAK_LABEL_RE.match(value):
        return True
    return bool(_LEAD_IN_RE.match(value)) and len(value) < 45


@dataclass(frozen=True)
class PageLink:
    href: str
    text: str
    aria_label: str
    heading: str = ""

    @property
    def best_label(self) -> str:
        """The most informative human label available for this link.

        aria-label wins because Isomer writes the full document title there
        even when the visible anchor text has been truncated for layout. Where
        both the label and the text are boilerplate - "Download", "this
        circular", "refer to this guide" - the heading the link sits under is
        usually the document's actual name, so it is tried before falling back
        to the filename.
        """
        # The candidate is returned intact, not stripped: the caller runs
        # split_label on it and needs the "[PDF, 5.1 MB]" suffix to survive
        # into metadata. Only the weakness test looks at the stripped form.
        for candidate in (self.aria_label, self.text):
            cleaned, _ = split_label(candidate)
            if cleaned and not is_weak_label(cleaned):
                return candidate
        if self.heading and not is_weak_label(self.heading):
            return self.heading
        return self.aria_label or self.text


def assign_titles(links: list["PageLink"]) -> list[tuple["PageLink", str, dict[str, str]]]:
    """Give every link on a page the best title available, page-wide.

    Per-link resolution is not enough, because the heading fallback is only
    trustworthy when the heading identifies *one* document. A "Codes of
    Practice" heading sitting over three PDFs names the group, not any member
    of it: using it would produce records that cannot be told apart, which is
    worse than three ugly filenames. So a heading is claimed only when exactly
    one link on the page sits under it - counting every link, not only the
    weakly-labelled ones, because a heading shared with a well-named sibling
    is just as ambiguous. Everything else falls back to a name derived from
    the filename, which at least differs per document.

    Returns ``(link, title, hints)`` in the order given.
    """
    provisional: list[tuple[PageLink, str, dict[str, str]]] = []
    heading_claims: dict[str, int] = {}

    for link in links:
        title, hints = split_label(link.aria_label)
        if is_weak_label(title):
            from_text, text_hints = split_label(link.text)
            if is_weak_label(from_text):
                title, hints = "", hints or text_hints
            else:
                title, hints = from_text, text_hints or hints
        if link.heading:
            heading_claims[link.heading] = heading_claims.get(link.heading, 0) + 1
        provisional.append((link, title, hints))

    resolved: list[tuple[PageLink, str, dict[str, str]]] = []
    for link, title, hints in provisional:
        if not title:
            if (
                link.heading
                and not is_weak_label(link.heading)
                and heading_claims.get(link.heading) == 1
            ):
                title = link.heading
            else:
                title = (
                    infer_name_from_link(None, link.href)
                    or clean_text(link.text)
                    or clean_text(link.heading)
                )
        resolved.append((link, title, hints))
    return resolved


class _ScopedLinkParser(HTMLParser):
    """Collect anchors, optionally restricted to a subtree by id.

    ``utils.html_links`` already scopes by element id, but it discards
    attributes. Isomer keeps the real document title in ``aria-label``, so this
    parser keeps attributes too.
    """

    def __init__(self, *, element_id: str | None = None) -> None:
        super().__init__(convert_charrefs=True)
        self._target_id = element_id
        self._depth = 0 if element_id else 1  # no id => whole document is in scope
        self._in_a = False
        self._href: str | None = None
        self._aria: str = ""
        self._parts: list[str] = []
        self._heading: str = ""
        self._in_heading = False
        self._heading_parts: list[str] = []
        self.links: list[PageLink] = []

    def _in_scope(self) -> bool:
        return self._depth > 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {k.lower(): (v or "") for k, v in attrs}

        if self._target_id is not None:
            if self._depth == 0:
                if attr_map.get("id") == self._target_id:
                    self._depth = 1
                return
            self._depth += 1

        if not self._in_scope():
            return

        if tag.lower() in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self._in_heading = True
            self._heading_parts = []
            return

        if tag.lower() != "a":
            return

        self._in_a = True
        self._href = attr_map.get("href") or None
        self._aria = attr_map.get("aria-label") or attr_map.get("title") or ""
        self._parts = []

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # Void elements (<img/>, <br/>) must not inflate the nesting depth.
        if self._target_id is not None and self._depth > 0:
            attr_map = {k.lower(): (v or "") for k, v in attrs}
            if self._depth == 0 and attr_map.get("id") == self._target_id:
                self._depth = 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in ("h1", "h2", "h3", "h4", "h5", "h6") and self._in_heading:
            self._heading = clean_text("".join(self._heading_parts))
            self._in_heading = False
            self._heading_parts = []

        if tag.lower() == "a" and self._in_a and self._href:
            self.links.append(
                PageLink(
                    href=self._href,
                    text=clean_text("".join(self._parts)),
                    aria_label=clean_text(self._aria),
                    heading=self._heading,
                )
            )
            self._in_a = False
            self._href = None
            self._aria = ""
            self._parts = []

        if self._target_id is not None and self._depth > 0:
            self._depth -= 1

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._parts.append(data)
        elif self._in_heading:
            self._heading_parts.append(data)


def extract_page_links(
    html: str,
    *,
    base_url: str,
    element_id: str | None = "main-content",
) -> list[PageLink]:
    """Extract anchors from ``html``, preferring the ``element_id`` subtree.

    If the scoped pass finds nothing the page layout has changed (or the id
    differs on this site), so fall back to the whole document rather than
    silently returning zero records.
    """
    parser = _ScopedLinkParser(element_id=element_id)
    parser.feed(html)
    links = parser.links

    if not links and element_id is not None:
        fallback = _ScopedLinkParser(element_id=None)
        fallback.feed(html)
        links = fallback.links

    return [
        PageLink(
            href=urljoin(base_url, link.href),
            text=link.text,
            aria_label=link.aria_label,
            heading=link.heading,
        )
        for link in links
    ]


_TITLE_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_HEAD_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def extract_page_title(html: str) -> str | None:
    """Best-effort human title for a page: first <h1>, else <title>."""
    for pattern in (_TITLE_RE, _HEAD_TITLE_RE):
        match = pattern.search(html or "")
        if not match:
            continue
        text = clean_text(_TAG_RE.sub(" ", match.group(1)))
        # Isomer suffixes <title> with the agency name; keep the leading part.
        text = text.split("|")[0].strip() if pattern is _HEAD_TITLE_RE else text
        if text:
            return text
    return None


def split_label(label: str) -> tuple[str, dict[str, str]]:
    """Split ``"Approved Document [PDF, 5.1 MB] (opens in new tab)"``.

    Returns the clean title and any file hints found, so the size and declared
    type survive into record metadata instead of polluting the name.
    """
    text = _LABEL_NOISE_RE.sub("", clean_text(label))
    hints: dict[str, str] = {}

    match = _FILE_HINT_RE.search(text) or _ORPHAN_HINT_RE.search(text)
    if match:
        hints["declared_file_type"] = match.group("kind").lower()
        hints["declared_file_size"] = clean_text(match.group("size"))
        # Everything after the hint is description, not title.
        text = text[: match.start()].strip()

    return text.strip(" -–—,;:"), hints


# ---------------------------------------------------------------------------
# Sitemap
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SitemapEntry:
    url: str
    lastmod: str | None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_sitemap(xml_text: str) -> tuple[list[SitemapEntry], list[str]]:
    """Parse a sitemap into (page entries, nested sitemap urls).

    Two things this has to cope with in the wild. Sitemaps come in two flavours
    - a ``<urlset>`` of pages and a ``<sitemapindex>`` of further sitemaps -
    and a caller that handles only the first silently returns nothing on the
    second (MOM publishes an index; treating it as a urlset yields zero pages
    and looks like a working crawler finding nothing). Namespaces are also
    declared inconsistently, so matching is on the local tag name rather than
    a fixed namespace.

    ``lastmod`` is what lets a crawler report *when* a page last changed
    without downloading anything - the cheapest freshness signal there is,
    where the site bothers to maintain it. Several do not: NEA stamps every
    entry with the same 2018 date. Check before trusting it.
    """
    entries: list[SitemapEntry] = []
    nested: list[str] = []
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return entries, nested

    for node in root.iter():
        tag = _local(node.tag)
        if tag not in ("url", "sitemap"):
            continue
        loc = lastmod = None
        for child in node:
            name = _local(child.tag)
            if name == "loc":
                loc = (child.text or "").strip()
            elif name == "lastmod":
                lastmod = (child.text or "").strip()
        if not loc:
            continue
        if tag == "sitemap":
            nested.append(loc)
        else:
            entries.append(SitemapEntry(url=loc, lastmod=lastmod or None))
    return entries, nested


# ---------------------------------------------------------------------------
# Next.js flight payload (collection listing pages)
# ---------------------------------------------------------------------------

_FLIGHT_CHUNK_RE = re.compile(
    r'self\.__next_f\.push\(\[\d+,\s*"((?:[^"\\]|\\.)*)"\s*\]\)'
)

_COLLECTION_ITEM_RE = re.compile(
    r'"referenceLinkHref"\s*:\s*"(?P<href>[^"]+)"'
    r'[\s\S]{0,400}?"itemTitle"\s*:\s*"(?P<title>(?:[^"\\]|\\.)*)"'
    r'[\s\S]{0,120}?"formattedDate"\s*:\s*"(?P<date>[^"]*)"'
)


def decode_flight_payload(html: str) -> str:
    """Concatenate and unescape the Next.js RSC payload chunks in ``html``.

    A collection listing page renders only its first page of results into the
    DOM, but ships the full dataset in these ``self.__next_f.push`` chunks.
    Each chunk is a JavaScript string literal, so ``json.loads`` on the quoted
    literal restores the original text exactly - far more reliable than trying
    to write a regex that copes with layered backslash escaping.
    """
    parts: list[str] = []
    for match in _FLIGHT_CHUNK_RE.finditer(html or ""):
        try:
            parts.append(json.loads('"' + match.group(1) + '"'))
        except (ValueError, json.JSONDecodeError):
            continue
    return "".join(parts)


@dataclass(frozen=True)
class CollectionItem:
    href: str
    title: str
    formatted_date: str


def extract_collection_items(html: str) -> list[CollectionItem]:
    """Every item in an Isomer collection listing, not just the visible page."""
    payload = decode_flight_payload(html)
    if not payload:
        return []

    items: list[CollectionItem] = []
    seen: set[tuple[str, str]] = set()
    for match in _COLLECTION_ITEM_RE.finditer(payload):
        href = match.group("href").strip()
        title = clean_text(match.group("title").replace("\\/", "/"))
        date = clean_text(match.group("date"))
        key = (href, date)
        if not href or key in seen:
            continue
        seen.add(key)
        items.append(CollectionItem(href=href, title=title, formatted_date=date))
    return items


# ---------------------------------------------------------------------------
# Crawler base
# ---------------------------------------------------------------------------


def _as_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return []


def _canon(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def strip_query(url: str) -> str:
    """Drop the query string for record-identity purposes.

    Sitefinity and Sitecore hang a cache-busting version parameter off every
    asset (``?sfvrsn=``, ``?hash=``, ``?la=en``). Records are keyed on the URL,
    so leaving it in makes the same document look new whenever the CMS
    re-stamps it, and the daily archive diff fills with phantom adds and
    removes. The fetchable URL is kept in metadata.
    """
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


class _IsomerBase:
    """Shared config plumbing and HTTP session handling."""

    name = "isomer"

    def _config(self, ctx: RunContext) -> dict[str, Any]:
        return ctx.get_crawler_config(self.name)

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
        delay = float(cfg.get("request_delay_seconds", 0.25))
        jitter = float(cfg.get("request_jitter_seconds", 0.10))
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
            return resp.text or ""
        except Exception as exc:  # network/HTTP failures should not abort the run
            if ctx.debug:
                print(f"[{self.name}] error fetching {url}: {exc}")
            return None


class IsomerSectionCrawler(_IsomerBase):
    """Harvest documents from every page under a set of site sections.

    A subclass supplies only ``name``; everything that varies between sections
    (which path prefixes, whether to keep the pages themselves, which hosts
    count as documents) lives in ``config/settings.yaml`` so that adding a
    section is a config change rather than a code change.

    Recognised config keys
    ----------------------
    sitemap_url            Absolute URL of the site's sitemap.
    path_prefixes          Only sitemap paths starting with one of these are
                           crawled. This is the scope decision, and mirrors
                           one row of the approved scope spec.
    exclude_path_patterns  Regexes applied to the path; matches are skipped.
    emit_page_records      Emit a record for the page itself. Set this when the
                           scope spec asks for the page's web-text, not only
                           the files it links to.
    document_extensions    File extensions treated as documents.
    document_hosts         Hosts whose files count as this agency's documents.
                           Defaults to the sitemap host plus the Isomer asset
                           host derived from links actually seen.
    external_document_hosts
                           Hosts that are not the agency but hold primary
                           material worth recording - e.g. sso.agc.gov.sg for
                           Singapore legislation. Recorded with
                           meta.record_kind = "external_reference".
    content_element_id     Element id to scope link extraction to.
    max_pages              Safety cap on pages fetched.
    max_total_records      Safety cap on records emitted.
    """

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = self._config(ctx)

        sitemap_url = str(cfg.get("sitemap_url", "")).strip()
        prefixes = _as_list(cfg.get("path_prefixes"))
        if not sitemap_url or not prefixes:
            return []

        excludes = [re.compile(p) for p in _as_list(cfg.get("exclude_path_patterns"))]
        doc_exts = {
            e.lower() if e.startswith(".") else f".{e.lower()}"
            for e in (_as_list(cfg.get("document_extensions")) or DEFAULT_DOCUMENT_EXTENSIONS)
        }
        doc_hosts = {h.lower() for h in _as_list(cfg.get("document_hosts"))}
        doc_prefixes = [p.lower() for p in _as_list(cfg.get("document_path_prefixes"))]
        ext_hosts = {h.lower() for h in _as_list(cfg.get("external_document_hosts"))}
        strip_doc_query = bool(cfg.get("strip_document_query", True))
        content_element_id = str(cfg.get("content_element_id", "main-content")).strip() or None
        emit_pages = bool(cfg.get("emit_page_records", True))
        max_pages = int(cfg.get("max_pages", 1000))
        max_records = int(cfg.get("max_total_records", 50000))
        read_collection_payload = bool(cfg.get("read_collection_payload", True))

        session = self._session(ctx)

        sitemap_text = self._fetch(session, sitemap_url, ctx=ctx, cfg=cfg)
        if not sitemap_text:
            return []

        site_host = (urlparse(sitemap_url).netloc or "").lower()
        if not doc_hosts:
            doc_hosts = {site_host, "isomer-user-content.by.gov.sg"}

        found, nested = parse_sitemap(sitemap_text)
        # A sitemap index lists further sitemaps rather than pages. Following
        # them is not optional: several agencies publish only an index, and a
        # crawler that stops at the top level reports zero pages while looking
        # perfectly healthy.
        for child_url in nested[: int(cfg.get("max_child_sitemaps", 30))]:
            child_text = self._fetch(session, child_url, ctx=ctx, cfg=cfg)
            if not child_text:
                continue
            child_entries, _ = parse_sitemap(child_text)
            found.extend(child_entries)

        entries = [e for e in found if _path_matches(e.url, prefixes, excludes)]
        entries.sort(key=lambda e: e.url)

        if not entries and ctx.debug:
            print(
                f"[{self.name}] sitemap yielded no pages under {prefixes}; "
                "check the prefixes against the sitemap's actual paths"
            )

        out: list[UrlRecord] = []
        seen: set[str] = set()

        for entry in entries[:max_pages]:
            html = self._fetch(session, entry.url, ctx=ctx, cfg=cfg)
            if html is None:
                continue

            page_title = extract_page_title(html)

            if emit_pages:
                canon_page = _canon(entry.url)
                if canon_page and canon_page not in seen:
                    seen.add(canon_page)
                    out.append(
                        ctx.make_record(
                            url=canon_page,
                            name=page_title or infer_name_from_link(None, canon_page),
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            publish_date=entry.lastmod,
                            meta={
                                "record_kind": "page",
                                "file_ext": "html",
                                "sitemap_lastmod": entry.lastmod,
                            },
                        )
                    )

            links: list[PageLink] = extract_page_links(
                html, base_url=entry.url, element_id=content_element_id
            )

            # Listing pages hide the bulk of their items in the flight payload.
            # Folding those in here means a section that happens to contain a
            # collection page is not silently under-collected.
            if read_collection_payload:
                for item in extract_collection_items(html):
                    links.append(
                        PageLink(
                            href=urljoin(entry.url, item.href),
                            text=item.title,
                            aria_label=item.title,
                        )
                    )

            for link, title, hints in assign_titles(links):
                if len(out) >= max_records:
                    break

                canon = _canon(link.href)
                if not canon or canon in seen:
                    continue

                host = (urlparse(canon).netloc or "").lower()
                path = (urlparse(canon).path or "").lower()
                is_doc = path_ext(canon) in doc_exts and host in doc_hosts
                # Where a CMS keeps every published file under one asset path
                # - Sitecore's /-/media/, Sitefinity's /docs/default-source/,
                # AEM's /content/dam/ - that prefix is a far sharper filter
                # than the extension, because navigation never points into it.
                if is_doc and doc_prefixes:
                    is_doc = any(path.startswith(p) for p in doc_prefixes)
                # Naming a host under external_document_hosts is the decision
                # that its links matter; extension is not part of it. Statutes
                # Online serves legislation at extensionless URLs, and
                # requiring ".pdf" would drop exactly the primary law the
                # codes are written under.
                is_external = host in ext_hosts

                if not is_doc and not is_external:
                    continue

                identity = strip_query(canon) if strip_doc_query else canon
                if identity in seen:
                    continue
                seen.add(identity)
                seen.add(canon)
                meta: dict[str, Any] = {
                    "record_kind": "document" if is_doc else "external_reference",
                    "discovered_from": entry.url,
                    "discovered_from_title": page_title,
                    "sitemap_lastmod": entry.lastmod,
                }
                if is_doc:
                    meta["file_ext"] = path_ext(identity).lstrip(".")
                if identity != canon:
                    meta["fetch_url"] = canon
                meta.update(hints)

                out.append(
                    ctx.make_record(
                        url=identity,
                        name=title or infer_name_from_link(link.text, canon),
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        publish_date=None,
                        meta=meta,
                    )
                )

            if len(out) >= max_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        return out


class IsomerCollectionCrawler(_IsomerBase):
    """Harvest a complete Isomer collection listing (circulars, news, etc.).

    The listing page shows ten items and offers pagination, but the response
    already contains every item. Reading the flight payload turns what looks
    like a forty-page walk into one request, and hands back the publication
    date for each item - which the visible HTML only shows for the current
    page.

    Recognised config keys
    ----------------------
    listing_urls        One or more collection listing pages.
    emit_page_records   Emit a record for the listing page itself.
    include_url_patterns / exclude_url_patterns
                        Regexes over the item URL, for the case where a
                        collection mixes categories and the scope spec only
                        calls for some of them.
    max_total_records   Safety cap.
    """

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = self._config(ctx)

        listing_urls = _as_list(cfg.get("listing_urls")) or _as_list(cfg.get("listing_url"))
        if not listing_urls:
            return []

        includes = [re.compile(p, re.I) for p in _as_list(cfg.get("include_url_patterns"))]
        excludes = [re.compile(p, re.I) for p in _as_list(cfg.get("exclude_url_patterns"))]
        emit_pages = bool(cfg.get("emit_page_records", True))
        max_records = int(cfg.get("max_total_records", 50000))

        session = self._session(ctx)
        out: list[UrlRecord] = []
        seen: set[str] = set()

        for listing_url in listing_urls:
            html = self._fetch(session, listing_url, ctx=ctx, cfg=cfg)
            if html is None:
                continue

            listing_title = extract_page_title(html)

            if emit_pages:
                canon_listing = _canon(listing_url)
                if canon_listing and canon_listing not in seen:
                    seen.add(canon_listing)
                    out.append(
                        ctx.make_record(
                            url=canon_listing,
                            name=listing_title or "Collection listing",
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            publish_date=None,
                            meta={"record_kind": "index_page", "file_ext": "html"},
                        )
                    )

            items = extract_collection_items(html)
            if not items and ctx.debug:
                print(
                    f"[{self.name}] no collection items in {listing_url}; "
                    "the payload shape may have changed"
                )

            for item in items:
                if len(out) >= max_records:
                    break

                canon = _canon(urljoin(listing_url, item.href))
                if not canon or canon in seen:
                    continue
                if includes and not any(p.search(canon) for p in includes):
                    continue
                if any(p.search(canon) for p in excludes):
                    continue

                seen.add(canon)
                title, hints = split_label(item.title)
                ext = path_ext(canon).lstrip(".")
                meta: dict[str, Any] = {
                    "record_kind": "document" if ext else "page",
                    "discovered_from": listing_url,
                    "collection": listing_title,
                    "date_raw": item.formatted_date,
                }
                if ext:
                    meta["file_ext"] = ext
                meta.update(hints)

                out.append(
                    ctx.make_record(
                        url=canon,
                        name=title or infer_name_from_link(None, canon),
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        publish_date=item.formatted_date or None,
                        meta=meta,
                    )
                )

            if len(out) >= max_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        return out


def _path_matches(
    url: str,
    prefixes: Iterable[str],
    excludes: Iterable[re.Pattern[str]],
) -> bool:
    path = urlparse(url).path or "/"
    if not any(path.startswith(prefix) for prefix in prefixes):
        return False
    return not any(pattern.search(path) for pattern in excludes)
