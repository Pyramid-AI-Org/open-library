"""Engines for sites that ship their data in the response but not in the DOM.

The two existing engines both assume the links are anchors. ``isomer.py`` reads
them from a sitemap-discovered page; ``spider.py`` follows them. Neither works
on a listing that renders client-side from an embedded payload or a JSON API -
and on the Singapore set that turned out to be the normal case rather than the
exception. Three sources were reported as fully crawled and near-empty for
exactly this reason:

* **Parliament** - 120 listing pages fetched, zero documents. The listings are
  a Next.js app; the item links are React handlers, not anchors, and the files
  are not under the document prefix the survey recorded.
* **EMA** - 94 records from a spider. Its listings are Angular, and the
  documents live behind AEM GraphQL persisted queries.
* **NParks** - 119 records. Pages server-rendered, listings not.

The lesson is worth stating plainly because it is not obvious: **"the page
fetch succeeded" and "the crawl worked" are different claims.** A crawler that
reports 120 pages and 0 documents has usually not found an empty section - it
has found the wrong extraction route. This module gives that route three
shapes.

Engines
-------

``FlightPayloadCrawler``
    One request. Concatenates the ``self.__next_f.push([n,"..."])`` chunks,
    JSON-unescapes each, and reads a named key out of the reassembled payload.
    When the server pre-loads the whole collection - Parliament's Order Paper
    ships all 2,168 records this way - there is no pagination to do at all.

``ServerActionCrawler``
    For the same framework when the payload holds only the first page. Replays
    the Next.js server action the pagination control fires, with the limit
    raised. Parliament's two other listings return 619 and 772 records in a
    single call each.

``ApiIndexCrawler``
    For a site with an out-of-band index: a JSON page list, a set of GraphQL
    persisted queries, or both. Walks whatever JSON comes back and records
    every asset path it finds, pairing each with the nearest title and date
    field above it in the tree.

All three take their record shape from config rather than hard-coding one, so
a new collection is a settings block rather than another module.

Only ``requests`` and the standard library are used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    normalize_publish_date,
    path_ext,
    sleep_seconds,
)


# --------------------------------------------------------------------------
# Flight payload decoding
# --------------------------------------------------------------------------

_CHUNK = re.compile(r'self\.__next_f\.push\(\[\d+,\s*"((?:[^"\\]|\\.)*)"\s*\]\)')


def decode_flight_payload(html: str) -> str:
    """Reassemble a Next.js flight payload from its script chunks.

    Each chunk is a JSON string literal, so the escaping has to be undone one
    chunk at a time - concatenating first and unescaping after corrupts any
    chunk boundary that falls inside an escape sequence.
    """
    parts: list[str] = []
    for match in _CHUNK.finditer(html):
        try:
            parts.append(json.loads('"' + match.group(1) + '"'))
        except ValueError:
            # A truncated final chunk is normal on a partial response. Keep
            # what decoded rather than losing the whole payload.
            continue
    return "".join(parts)


def balanced_object(text: str, start: int) -> str | None:
    """Return the JSON object beginning at ``start``, brace-matched.

    Payloads are not valid JSON as a whole - they are a stream of numbered
    lines - so the object of interest has to be cut out by hand. Quote and
    escape state is tracked so a brace inside a string does not end the scan.
    """
    if start >= len(text) or text[start] != "{":
        return None

    depth = 0
    in_string = False
    escaped = False

    for i in range(start, len(text)):
        ch = text[i]
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if in_string:
            if ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def extract_keyed_object(payload: str, key: str) -> Any | None:
    """Find ``"<key>":`` in a payload and parse the object that follows."""
    marker = '"%s":' % key
    index = payload.find(marker)
    if index < 0:
        return None
    raw = balanced_object(payload, index + len(marker))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


def first_collection(payload: str) -> Any | None:
    """Fall back to the first ``{"meta":{...},"data":[...]}`` object present.

    Payload key names change between builds far more often than the collection
    shape does. When the configured key is missing this recovers the data
    anyway - and the crawler says it had to, so the config can be corrected.
    """
    index = payload.find('{"meta":{"filter_count"')
    if index < 0:
        return None
    raw = balanced_object(payload, index)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


# --------------------------------------------------------------------------
# Record mapping
# --------------------------------------------------------------------------

_TITLE_KEYS: tuple[str, ...] = (
    "fileTitle",
    "circularTitle",
    "description",
    "title",
    "documentTitle",
    "publicationTitle",
    "formTitle",
    "name",
    "heading",
    "label",
    "fileName",
)

_DATE_KEYS: tuple[str, ...] = (
    "date",
    "date_introduced",
    "publishedDate",
    "publishdate",
    "publishDate",
    "publication_date",
    "effectiveDate",
    "date_created",
)

_ASSET_PATH = re.compile(r"/(?:content/dam|api/media|docs/default-source|-/media)/[^\"'\s<>\\)]+")

_DOC_EXT = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|zip|csv|rtf|dwg|txt)$", re.IGNORECASE)

_IMAGE_EXT = re.compile(r"\.(png|jpe?g|gif|svg|webp|ico|bmp)$", re.IGNORECASE)


def _first_string(node: dict, keys: Iterable[str]) -> str | None:
    for key in keys:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return clean_text(re.sub(r"<[^>]+>", " ", value))
    return None


def filename_to_url_segment(filename: str) -> str:
    """Turn a stored filename into the segment the media route serves it under.

    Directus keeps the original filename, spaces and all, and serves the file
    at a path where the spaces have become hyphens. Rebuilding this by hand is
    the sort of thing that quietly produces a library full of 404s, so it is
    one function with one test rather than an inline expression in three
    crawlers.
    """
    return re.sub(r"\s+", "-", filename.strip())


@dataclass
class RecordShape:
    """How to read one item of a collection.

    ``url_field`` wins when present. Otherwise ``url_template`` is formatted
    against the item, which is what the Directus-style rows need: the file is a
    nested object and the public URL is built from its id and filename.
    """

    url_field: str | None = None
    url_template: str | None = None
    file_field: str | None = None
    title_fields: tuple[str, ...] = _TITLE_KEYS
    date_fields: tuple[str, ...] = _DATE_KEYS
    extra_file_fields: tuple[str, ...] = ()

    def urls_for(self, item: dict, base_url: str) -> list[tuple[str, dict]]:
        """Return (url, file-object) pairs for one item - usually one, but a
        record with a corrigendum has two."""
        out: list[tuple[str, dict]] = []

        if self.url_field:
            value = item.get(self.url_field)
            if isinstance(value, str) and value.strip():
                out.append((urljoin(base_url, value.strip()), item))

        for field_name in (self.file_field, *self.extra_file_fields):
            if not field_name:
                continue
            obj = item.get(field_name)
            if not isinstance(obj, dict):
                continue
            file_id = obj.get("id")
            filename = obj.get("filename_download") or obj.get("filename_disk")
            if not file_id or not isinstance(filename, str):
                continue
            template = self.url_template or "/api/media/{id}/{filename}"
            path = template.format(id=file_id, filename=filename_to_url_segment(filename))
            out.append((urljoin(base_url, path), obj))

        return out


def _title_for(item: dict, file_obj: dict, shape: RecordShape) -> str | None:
    parts: list[str] = []
    primary = _first_string(item, shape.title_fields)
    if primary:
        parts.append(primary)
    number = item.get("title")
    if isinstance(number, str) and number.strip() and number.strip() not in parts:
        # Bills carry the Bill number in `title` and the Bill name in
        # `description`. Both matter to a citation, so keep both.
        if primary and re.fullmatch(r"[\w/\-]{1,16}", number.strip()):
            parts.append("[%s]" % number.strip())
    if not parts:
        fallback = _first_string(file_obj, shape.title_fields)
        if fallback:
            parts.append(fallback)
    return " ".join(parts) if parts else None


# --------------------------------------------------------------------------
# Shared behaviour
# --------------------------------------------------------------------------


class _PayloadCrawlerBase:
    """Config plumbing shared by the three engines."""

    name: str = ""

    def _config(self, ctx: RunContext) -> dict[str, Any]:
        cfg = ctx.get_crawler_config(self.name)
        if not cfg:
            raise ValueError(
                "No settings for crawler %r under crawlers.%s.pages"
                % (self.name, ctx.source_id)
            )
        return cfg

    @staticmethod
    def _session(ctx: RunContext) -> requests.Session:
        http = ctx.get_http_config()
        session = requests.Session()
        user_agent = http.get("user_agent")
        if user_agent:
            session.headers["User-Agent"] = user_agent
        return session

    @staticmethod
    def _http_kwargs(ctx: RunContext) -> dict[str, Any]:
        http = ctx.get_http_config()
        return {
            "timeout_seconds": http.get("timeout_seconds", 30),
            "max_retries": http.get("max_retries", 3),
            "backoff_base_seconds": http.get("backoff_base_seconds", 1.0),
            "backoff_jitter_seconds": http.get("backoff_jitter_seconds", 0.4),
        }

    @staticmethod
    def _shape(cfg: dict[str, Any]) -> RecordShape:
        raw = cfg.get("record", {}) or {}
        return RecordShape(
            url_field=raw.get("url_field"),
            url_template=raw.get("url_template"),
            file_field=raw.get("file_field"),
            title_fields=tuple(raw.get("title_fields") or _TITLE_KEYS),
            date_fields=tuple(raw.get("date_fields") or _DATE_KEYS),
            extra_file_fields=tuple(raw.get("extra_file_fields") or ()),
        )

    def _records_from_items(
        self,
        ctx: RunContext,
        items: Iterable[dict],
        *,
        base_url: str,
        shape: RecordShape,
        found_on: str,
    ) -> list[UrlRecord]:
        records: list[UrlRecord] = []
        seen: set[str] = set()

        for item in items:
            if not isinstance(item, dict):
                continue
            date_value = _first_string(item, shape.date_fields)
            for url, file_obj in shape.urls_for(item, base_url):
                canonical = canonicalize_url(url)
                if not canonical or canonical in seen:
                    continue
                seen.add(canonical)
                title = _title_for(item, file_obj if isinstance(file_obj, dict) else {}, shape)
                if date_value:
                    title_with_date = "%s (%s)" % (title, date_value) if title else None
                else:
                    title_with_date = title
                records.append(
                    ctx.make_record(
                        url=canonical,
                        name=title_with_date or title,
                        discovered_at_utc=ctx.started_at_utc,
                        source=f"{ctx.source_id}.{self.name}",
                        publish_date=normalize_publish_date(date_value),
                        meta={
                            "kind": "document",
                            "file_type": path_ext(canonical).lstrip("."),
                            "found_on": found_on,
                            "size_bytes": file_obj.get("filesize") or item.get("fileSize")
                            if isinstance(file_obj, dict)
                            else None,
                        },
                    )
                )
        return records


# --------------------------------------------------------------------------
# Engine 1: the whole collection is already in the page
# --------------------------------------------------------------------------


class FlightPayloadCrawler(_PayloadCrawlerBase):
    """One GET, one payload, the entire collection.

    Settings::

        order_paper:
          start_url: "https://www.parliament.gov.sg/parliamentary-business/order-paper"
          payload_key: "initData"
          record:
            url_field: "fileUrl"
    """

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = self._config(ctx)
        start_url = cfg["start_url"]
        payload_key = cfg.get("payload_key", "initData")
        shape = self._shape(cfg)
        session = self._session(ctx)

        response = get_with_retries(
            session,
            start_url,
            retry_statuses=(202, 429, 500, 502, 503, 504),
            **self._http_kwargs(ctx),
        )
        payload = decode_flight_payload(response.text)
        if not payload:
            print("[%s] EMPTY PAYLOAD - no flight chunks in the response. This "
                  "usually means a bot-mitigation interstitial answered instead "
                  "of the page." % self.name)
            return []

        collection = extract_keyed_object(payload, payload_key)
        if collection is None:
            collection = first_collection(payload)
            if collection is not None:
                print("[%s] payload_key %r not found; recovered the first "
                      "collection instead. Update the spec." % (self.name, payload_key))

        if not isinstance(collection, dict):
            print("[%s] NO COLLECTION in payload - extraction route is wrong, "
                  "not the section empty." % self.name)
            return []

        items = collection.get("data") or []
        expected = (collection.get("meta") or {}).get("filter_count")

        records = self._records_from_items(
            ctx, items, base_url=start_url, shape=shape, found_on=start_url
        )

        if expected and len(items) < expected:
            print("[%s] INCOMPLETE: payload held %d of %d records. This listing "
                  "paginates - use ServerActionCrawler." % (self.name, len(items), expected))

        records.append(
            ctx.make_record(
                url=canonicalize_url(start_url),
                name=cfg.get("label") or self.name.replace("_", " ").title(),
                discovered_at_utc=ctx.started_at_utc,
                source=f"{ctx.source_id}.{self.name}",
                publish_date=None,
                meta={"kind": "page", "file_type": "html"},
            )
        )
        records.sort(key=lambda r: r.url)
        return records


# --------------------------------------------------------------------------
# Engine 2: replay the pagination action with the limit raised
# --------------------------------------------------------------------------


class ServerActionCrawler(_PayloadCrawlerBase):
    """Fetch a whole collection through a Next.js server action.

    The action id is a build hash. It changes on every deploy, which is why it
    belongs in settings rather than in code, and why a stale one must fail
    loudly: the server answers HTTP 500, and a crawler that swallowed that
    would record zero documents and look healthy doing it.

    Settings::

        bills_introduced:
          start_url: "https://www.parliament.gov.sg/parliamentary-business/bills-introduced"
          action_id: "7f20370fa7fced498f1fcafee03b02cce4cebdbd3c"
          form_fields: {_1_name: "", _1_title: "", _1_yearOption: "", "0": '[null,"$K1"]'}
          offset_field: "_1_offset"
          limit_field: "_1_limit"
          page_size: 1000
          record:
            file_field: "file"
            extra_file_fields: ["corrigenda"]
            url_template: "/api/media/{id}/{filename}"
    """

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = self._config(ctx)
        start_url = cfg["start_url"]
        action_id = cfg.get("action_id")
        if not action_id:
            raise ValueError(
                "[%s] no action_id in settings. Read it off a live pagination "
                "request - it is a build hash and cannot be guessed." % self.name
            )

        offset_field = cfg.get("offset_field", "_1_offset")
        limit_field = cfg.get("limit_field", "_1_limit")
        page_size = int(cfg.get("page_size", 1000))
        shape = self._shape(cfg)
        session = self._session(ctx)
        http = self._http_kwargs(ctx)

        items: list[dict] = []
        expected: int | None = None
        offset = 0

        while True:
            form: dict[str, str] = {
                str(k): "" if v is None else str(v)
                for k, v in (cfg.get("form_fields") or {}).items()
            }
            form[offset_field] = str(offset)
            form[limit_field] = str(page_size)

            # Field order is load-bearing. React resolves the argument slots
            # ("0", "1", ...) against the "$K<n>" groups assembled from the
            # `_<n>_*` entries, and it reads the parts in order: an argument
            # that arrives before its group sees an empty group. The app sends
            # every group entry first and the argument slots last, so a request
            # with "0" in the middle silently loses offset and limit and the
            # server answers page one every time - with a correct
            # filter_count, which is what makes it look like a working crawler
            # stuck on the first page.
            ordered = sorted(form.items(), key=lambda kv: kv[0].isdigit())

            response = session.post(
                start_url,
                files=[(k, (None, v)) for k, v in ordered],  # multipart, as the app sends
                headers={"next-action": action_id, "Accept": "text/x-component"},
                timeout=http["timeout_seconds"],
            )
            if response.status_code == 500:
                raise RuntimeError(
                    "[%s] server action returned HTTP 500. The action_id is "
                    "almost certainly stale after a site deploy - re-capture it. "
                    "Refusing to report an empty collection." % self.name
                )
            response.raise_for_status()

            collection = first_collection(response.text)
            if not isinstance(collection, dict):
                break

            batch = collection.get("data") or []
            if expected is None:
                expected = (collection.get("meta") or {}).get("filter_count")
            items.extend(x for x in batch if isinstance(x, dict))

            if not batch or expected is None or len(items) >= expected:
                break
            offset += len(batch)
            sleep_seconds(float(cfg.get("delay_seconds", 0.4)))

        if expected is not None and len(items) < expected:
            print("[%s] INCOMPLETE: %d of %d records." % (self.name, len(items), expected))

        records = self._records_from_items(
            ctx, items, base_url=start_url, shape=shape, found_on=start_url
        )
        records.append(
            ctx.make_record(
                url=canonicalize_url(start_url),
                name=cfg.get("label") or self.name.replace("_", " ").title(),
                discovered_at_utc=ctx.started_at_utc,
                source=f"{ctx.source_id}.{self.name}",
                publish_date=None,
                meta={"kind": "page", "file_type": "html"},
            )
        )
        records.sort(key=lambda r: r.url)
        return records


# --------------------------------------------------------------------------
# Engine 3: an out-of-band index, JSON all the way down
# --------------------------------------------------------------------------


def walk_for_assets(
    node: Any,
    *,
    base_url: str,
    inherited_title: str | None = None,
    inherited_date: str | None = None,
    accumulator: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Collect every asset path in an arbitrary JSON tree.

    The pairing rule is deliberately simple: an asset takes the nearest title
    and date found at or above its own level. Anything cleverer needs to know
    the schema, and the point of this engine is to work without knowing it.
    Images are dropped - a logo is not a document, and letting them through
    produces a coverage number that looks good and means nothing.
    """
    if accumulator is None:
        accumulator = {}

    if isinstance(node, list):
        for child in node:
            walk_for_assets(
                child,
                base_url=base_url,
                inherited_title=inherited_title,
                inherited_date=inherited_date,
                accumulator=accumulator,
            )
        return accumulator

    if not isinstance(node, dict):
        return accumulator

    title = _first_string(node, _TITLE_KEYS) or inherited_title
    date_value = _first_string(node, _DATE_KEYS) or inherited_date

    for value in node.values():
        if isinstance(value, str):
            for match in _ASSET_PATH.finditer(value):
                path = match.group(0)
                if _IMAGE_EXT.search(path) or not _DOC_EXT.search(path):
                    continue
                url = canonicalize_url(urljoin(base_url, path))
                if url and url not in accumulator:
                    accumulator[url] = {"title": title, "date": date_value}
        else:
            walk_for_assets(
                value,
                base_url=base_url,
                inherited_title=title,
                inherited_date=date_value,
                accumulator=accumulator,
            )

    return accumulator


class ApiIndexCrawler(_PayloadCrawlerBase):
    """Harvest a collection from a JSON endpoint of unknown shape.

    Settings::

        licences:
          base_url: "https://www.ema.gov.sg"
          endpoints:
            - "/graphql/execute.json/corporate/licensing-information"
          found_on: "https://www.ema.gov.sg/regulations-licences/licences"
          tolerate_statuses: [500]
    """

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = self._config(ctx)
        base_url = cfg["base_url"].rstrip("/")
        endpoints = cfg.get("endpoints") or []
        found_on = cfg.get("found_on") or base_url
        tolerated = set(int(s) for s in (cfg.get("tolerate_statuses") or ()))
        session = self._session(ctx)
        http = self._http_kwargs(ctx)

        assets: dict[str, dict[str, Any]] = {}

        for endpoint in endpoints:
            url = endpoint if endpoint.startswith("http") else base_url + endpoint
            try:
                response = get_with_retries(session, url, **http)
            except requests.HTTPError as exc:
                status = getattr(exc.response, "status_code", None)
                if status in tolerated:
                    # EMA's statistics-list answers 500. That is a server fault
                    # with a documented workaround, not an empty collection -
                    # say so rather than recording nothing and moving on.
                    print("[%s] endpoint %s returned HTTP %s (tolerated). Its "
                          "records are expected from another row; if they are "
                          "missing, this is why." % (self.name, endpoint, status))
                    continue
                raise

            try:
                payload = response.json()
            except ValueError:
                print("[%s] endpoint %s did not return JSON. On AEM a `.model.json` "
                      "suffix is silently ignored and the HTML page is served "
                      "instead - check the URL." % (self.name, endpoint))
                continue

            walk_for_assets(payload, base_url=base_url, accumulator=assets)
            sleep_seconds(float(cfg.get("delay_seconds", 0.3)))

        records = [
            ctx.make_record(
                url=url,
                name=info.get("title"),
                discovered_at_utc=ctx.started_at_utc,
                source=f"{ctx.source_id}.{self.name}",
                publish_date=normalize_publish_date(info.get("date")),
                meta={
                    "kind": "document",
                    "file_type": path_ext(url).lstrip("."),
                    "found_on": found_on,
                },
            )
            for url, info in assets.items()
        ]

        if not records:
            print("[%s] zero documents from %d endpoint(s). Before accepting "
                  "that, check whether the index moved: /graphql/list.json "
                  "enumerates persisted queries on AEM." % (self.name, len(endpoints)))

        records.sort(key=lambda r: r.url)
        return records


__all__ = [
    "ApiIndexCrawler",
    "FlightPayloadCrawler",
    "RecordShape",
    "ServerActionCrawler",
    "balanced_object",
    "decode_flight_payload",
    "extract_keyed_object",
    "filename_to_url_segment",
    "first_collection",
    "walk_for_assets",
]
