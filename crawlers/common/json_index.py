"""Pages listed by a JSON index rather than a sitemap.

Some sites publish a complete, machine-readable catalogue of their resource
*pages* - every title, type and URL - and then put the actual document one hop
further, on the page itself. WSH Council is the case this was written for:
`/wshc/api/tal/Resources/GetJsonWshcResourceList` returns all 1,688 resources
in a single response, and each entry points at a detail page whose body holds
the PDF.

That is the sitemap engine's shape with a different source for the page list,
so this engine changes only that: it overrides
``IsomerSectionCrawler._page_entries`` and inherits the per-page document
extraction, the host and prefix rules, the budget accounting and the record
shape unchanged.

Why a JSON index is worth preferring when one exists: it is complete by
construction, it carries the site's own taxonomy - so a scope can be expressed
in the publisher's categories rather than guessed from URL prefixes - and it
costs one request instead of a paginated walk.

Settings::

    codes_of_practice:
      engine: json_index
      index_url: "https://www.tal.sg/wshc/api/tal/Resources/GetJsonWshcResourceList"
      url_field: "url"
      date_field: "date"
      # Keep only entries whose `category_field` value is in `include_categories`.
      category_field: "resourceChildTypeName"
      include_categories: ["Codes of Practice", "WSH Guidelines"]
"""

from __future__ import annotations

import json
from typing import Any

import requests

from crawlers.base import RunContext, normalize_publish_date
from crawlers.common.isomer import (
    IsomerSectionCrawler,
    SitemapEntry,
    _as_list,
    _path_matches,
)


def _decode_index(text: str) -> list[dict]:
    """Parse the index body into a list of entries.

    Handles the double-encoded case: some endpoints return a JSON *string*
    whose content is the JSON array, so one decode yields a str rather than a
    list. Missing that returns zero entries from a perfectly good response.
    """
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return []
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (ValueError, TypeError):
            return []
    if isinstance(data, dict):
        for key in ("data", "items", "results", "records"):
            inner = data.get(key)
            if isinstance(inner, list):
                data = inner
                break
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


class JsonIndexSectionCrawler(IsomerSectionCrawler):
    """Walk pages named by a JSON index, collecting the documents they hold."""

    def _page_entries(
        self,
        ctx: RunContext,
        *,
        cfg: dict[str, Any],
        session: requests.Session,
        prefixes: list[str],
        excludes: list,
    ) -> list[SitemapEntry] | None:
        index_url = str(cfg.get("index_url", "")).strip()
        if not index_url:
            return None

        body = self._fetch(session, index_url, ctx=ctx, cfg=cfg)
        if not body:
            return None

        raw = _decode_index(body)
        if not raw:
            # A readable response that yields no entries means the shape
            # changed, not that the catalogue emptied. Say so rather than
            # reporting a healthy-looking zero.
            print(
                "[%s] index at %s returned no entries. The response shape has "
                "probably changed - check it before accepting an empty result."
                % (self.name, index_url)
            )
            return None

        url_field = str(cfg.get("url_field", "url"))
        date_field = str(cfg.get("date_field", "") or "")
        category_field = str(cfg.get("category_field", "") or "")
        include = {
            str(c).strip().lower()
            for c in _as_list(cfg.get("include_categories"))
        }
        exclude = {
            str(c).strip().lower()
            for c in _as_list(cfg.get("exclude_categories"))
        }

        entries: list[SitemapEntry] = []
        seen: set[str] = set()
        for item in raw:
            if category_field:
                category = str(item.get(category_field) or "").strip().lower()
                if include and category not in include:
                    continue
                if category in exclude:
                    continue

            url = str(item.get(url_field) or "").strip()
            if not url or url in seen:
                continue
            # Prefixes stay authoritative even when the index is: a scope is
            # reviewable only if one place decides what is in it.
            if prefixes and not _path_matches(url, prefixes, excludes):
                continue
            seen.add(url)

            lastmod = None
            if date_field:
                lastmod = normalize_publish_date(item.get(date_field))

            entries.append(SitemapEntry(url=url, lastmod=lastmod))

        entries.sort(key=lambda e: e.url)
        print(
            "[%s] index listed %d entries, %d in scope."
            % (self.name, len(raw), len(entries))
        )
        return entries
