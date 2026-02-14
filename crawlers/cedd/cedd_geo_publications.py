from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    path_ext,
)


@dataclass(frozen=True)
class _Link:
    href: str
    text: str


@dataclass(frozen=True)
class _Row:
    row_title: str | None
    links: list[_Link]


class _GeoPublicationsParser(HTMLParser):
    """Parse CEDD GEO publications table.

    Expected shape:
      table.colorTable.pdfTable
      columns: Title | Price
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_Row] = []

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_th = False
        self._td_index = -1

        self._in_a = False
        self._accessibility_depth = 0
        self._current_href: str | None = None
        self._current_link_text_parts: list[str] = []

        self._row_links: list[_Link] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {
            c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()
        }

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_table and t == "table":
            classes = self._class_set(attrs_map)
            if "colortable" in classes and "pdftable" in classes:
                self._in_table = True
                self._table_depth = 1
                return

        if self._in_table:
            self._table_depth += 1
        else:
            return

        if t == "tr":
            self._in_tr = True
            self._in_th = False
            self._td_index = -1
            self._row_links = []
            self._in_a = False
            self._accessibility_depth = 0
            self._current_href = None
            self._current_link_text_parts = []
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            return

        if t == "td":
            self._td_index += 1
            return

        # We only read anchors in first column (Title).
        if t == "a" and not self._in_th and self._td_index == 0:
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_link_text_parts = []
            return

        if t == "span" and self._in_a:
            classes = self._class_set(attrs_map)
            if "accessibility" in classes:
                self._accessibility_depth += 1

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_table:
            return

        self._table_depth -= 1
        if self._table_depth == 0:
            self._in_table = False
            self._in_tr = False
            self._in_th = False
            self._in_a = False
            self._accessibility_depth = 0
            return

        if not self._in_tr:
            return

        if t == "span" and self._in_a and self._accessibility_depth > 0:
            self._accessibility_depth -= 1
            return

        if t == "a" and self._in_a:
            href = (self._current_href or "").strip()
            text = clean_text("".join(self._current_link_text_parts))
            if href:
                self._row_links.append(_Link(href=href, text=text))
            self._in_a = False
            self._accessibility_depth = 0
            self._current_href = None
            self._current_link_text_parts = []
            return

        if t == "th":
            self._in_th = False
            return

        if t == "tr":
            self._in_tr = False
            if self._row_links:
                row_title = None
                for link in self._row_links:
                    if link.text:
                        row_title = link.text
                        break
                self.rows.append(_Row(row_title=row_title, links=list(self._row_links)))

            self._td_index = -1
            self._row_links = []
            self._in_a = False
            self._accessibility_depth = 0

    def handle_data(self, data: str) -> None:
        if not self._in_table or not self._in_tr or self._in_th:
            return
        if not self._in_a:
            return
        if self._accessibility_depth > 0:
            return
        self._current_link_text_parts.append(data)


class Crawler:
    """Crawl CEDD GEO publications page and emit PDF records."""

    name = "cedd_geo_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                f"{base_url}/eng/publications/geo/index.html",
            )
        ).strip()

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if ctx.debug:
            print(f"[{self.name}] Fetch -> {page_url}")

        resp = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        parser = _GeoPublicationsParser()
        parser.feed(resp.text)

        out: list[UrlRecord] = []
        seen: set[str] = set()

        for row in parser.rows:
            for link in row.links:
                abs_url = urljoin(page_url, link.href)
                abs_url = canonicalize_url(abs_url)
                if not abs_url:
                    continue
                if not abs_url.startswith(base_url + "/"):
                    continue
                if path_ext(abs_url) != ".pdf":
                    continue
                if abs_url in seen:
                    continue
                seen.add(abs_url)

                title = clean_text(link.text) or row.row_title

                out.append(
                    UrlRecord(
                        url=abs_url,
                        name=title or None,
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "title": title or None,
                            "discovered_from": page_url,
                        },
                    )
                )

                if len(out) >= max_total_records:
                    break
            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url, (r.meta.get("title") or "")))
        return out
