from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
)
from crawlers.cedd.cedd_stan_pah import crawl_stan_pah_subpage


_ISSUE_DATE_HEADERS = {"latest update", "last update"}


@dataclass(frozen=True)
class _DiscoveredLink:
    href: str
    text: str
    issue_date: str | None


class _PageParser(HTMLParser):
    """Extract anchors from #content and map row issue date from table headers."""

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self.links: list[_DiscoveredLink] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._table_depth = 0
        self._table_headers: list[str] = []

        self._in_tr = False
        self._row_has_th = False
        self._row_has_td = False
        self._current_row_cells: list[str] = []
        self._current_row_links: list[tuple[str, str]] = []

        self._in_cell = False
        self._cell_tag = ""
        self._cell_text_parts: list[str] = []

        self._in_a = False
        self._current_href: str | None = None
        self._current_link_text_parts: list[str] = []
        self._in_accessibility_span = False

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(value: str | None) -> set[str]:
        return {c.strip().lower() for c in (value or "").split() if c.strip()}

    def _in_content(self) -> bool:
        return self._content_depth > 0

    def _emit_non_table_link(self, href: str, text: str) -> None:
        self.links.append(_DiscoveredLink(href=href, text=text, issue_date=None))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if self._content_depth == 0 and attrs_map.get("id") == self._content_element_id:
            self._content_depth = 1
        elif self._content_depth > 0:
            self._content_depth += 1

        if not self._in_content():
            return

        if t == "table":
            self._table_depth += 1

        if t == "tr" and self._table_depth > 0:
            self._in_tr = True
            self._row_has_th = False
            self._row_has_td = False
            self._current_row_cells = []
            self._current_row_links = []
            return

        if t in ("th", "td") and self._in_tr:
            self._in_cell = True
            self._cell_tag = t
            self._cell_text_parts = []
            if t == "th":
                self._row_has_th = True
            else:
                self._row_has_td = True
            return

        if t == "a":
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_link_text_parts = []
            return

        if t == "span" and self._in_a:
            classes = self._class_set(attrs_map.get("class"))
            if "accessibility" in classes:
                self._in_accessibility_span = True

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_content():
            return

        if t == "span" and self._in_accessibility_span:
            self._in_accessibility_span = False
            return

        if t == "a" and self._in_a:
            href = clean_text(self._current_href or "")
            text = clean_text("".join(self._current_link_text_parts))
            if href:
                if self._in_tr and self._table_depth > 0:
                    self._current_row_links.append((href, text))
                else:
                    self._emit_non_table_link(href, text)

            self._in_a = False
            self._current_href = None
            self._current_link_text_parts = []
            self._in_accessibility_span = False
            return

        if t in ("th", "td") and self._in_cell and self._cell_tag == t:
            cell_text = clean_text("".join(self._cell_text_parts))
            self._current_row_cells.append(cell_text)
            self._in_cell = False
            self._cell_tag = ""
            self._cell_text_parts = []
            return

        if t == "tr" and self._in_tr:
            issue_date: str | None = None

            if self._row_has_th and not self._row_has_td:
                self._table_headers = [h.lower() for h in self._current_row_cells]
            elif self._row_has_td:
                for idx, hdr in enumerate(self._table_headers):
                    if hdr in _ISSUE_DATE_HEADERS and idx < len(self._current_row_cells):
                        candidate = clean_text(self._current_row_cells[idx])
                        issue_date = candidate or None
                        break

                for href, text in self._current_row_links:
                    self.links.append(
                        _DiscoveredLink(href=href, text=text, issue_date=issue_date)
                    )

            self._in_tr = False
            self._row_has_th = False
            self._row_has_td = False
            self._current_row_cells = []
            self._current_row_links = []
            return

        if t == "table" and self._table_depth > 0:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._table_headers = []

        if self._content_depth > 0:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._in_content():
            return

        if self._in_cell:
            self._cell_text_parts.append(data)

        if self._in_a and not self._in_accessibility_span:
            self._current_link_text_parts.append(data)


class Crawler:
    """Crawl CEDD Standards/Specifications/Handbooks pages for PDF links."""

    name = "cedd_standards_spec_handbooks_cost"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                f"{base_url}/eng/publications/standards-spec-handbooks-cost/index.html",
            )
        ).strip()
        scope_prefix = str(
            cfg.get("scope_prefix", "/eng/publications/standards-spec-handbooks-cost/")
        ).strip()

        exclude_urls_raw = cfg.get("exclude_urls", [])
        exclude_urls: set[str] = set()
        for u in exclude_urls_raw:
            cu = canonicalize_url(str(u), encode_spaces=True)
            if cu:
                exclude_urls.add(cu)

        content_element_id = str(cfg.get("content_element_id", "content")).strip()
        stan_pah_url = str(
            cfg.get(
                "stan_pah_url",
                f"{base_url}/eng/publications/standards-spec-handbooks-cost/stan-pah/index.html",
            )
        ).strip()
        max_pages = int(cfg.get("max_pages", 600))
        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        page_url_canon = canonicalize_url(page_url, encode_spaces=True)
        stan_pah_url_canon = canonicalize_url(stan_pah_url, encode_spaces=True)
        if not page_url_canon:
            return []

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        queue: deque[str] = deque([page_url_canon])
        seen_pages: set[str] = set()
        seen_pdfs: set[str] = set()
        processed_special_pages: set[str] = set()

        out: list[UrlRecord] = []

        while queue and len(seen_pages) < max_pages and len(out) < max_total_records:
            current_url = queue.popleft()
            if current_url in seen_pages:
                continue
            if current_url in exclude_urls:
                continue
            seen_pages.add(current_url)

            if ctx.debug:
                print(f"[{self.name}] Fetch -> {current_url}")

            try:
                resp = get_with_retries(
                    session,
                    current_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except requests.RequestException:
                continue

            parser = _PageParser(content_element_id=content_element_id)
            parser.feed(resp.text)

            for link in parser.links:
                abs_url = canonicalize_url(
                    urljoin(current_url, link.href),
                    encode_spaces=True,
                )
                if not abs_url:
                    continue

                parsed = urlparse(abs_url)
                if parsed.netloc.lower() != urlparse(base_url).netloc.lower():
                    continue

                # Special handling for PAH subpage in separate helper module.
                if stan_pah_url_canon and abs_url == stan_pah_url_canon:
                    if abs_url not in processed_special_pages:
                        processed_special_pages.add(abs_url)
                        if ctx.debug:
                            print(f"[{self.name}] Fetch (special: stan-pah) -> {abs_url}")
                        try:
                            special_rows = crawl_stan_pah_subpage(
                                session=session,
                                page_url=abs_url,
                                base_url=base_url,
                                discovered_at_utc=ctx.started_at_utc,
                                source=self.name,
                                timeout_seconds=timeout_seconds,
                                max_retries=max_retries,
                                backoff_base_seconds=backoff_base_seconds,
                                backoff_jitter_seconds=backoff_jitter_seconds,
                                content_element_id=content_element_id,
                            )
                        except requests.RequestException:
                            special_rows = []

                        for rec in special_rows:
                            if rec.url in seen_pdfs:
                                continue
                            seen_pdfs.add(rec.url)
                            out.append(rec)
                            if len(out) >= max_total_records:
                                break
                    continue

                if abs_url in exclude_urls:
                    continue

                ext = path_ext(abs_url)
                if ext == ".pdf":
                    if abs_url in seen_pdfs:
                        continue
                    seen_pdfs.add(abs_url)

                    name = clean_text(link.text) or infer_name_from_link(link.text, abs_url)

                    meta: dict[str, str | None] = {
                        "discovered_from": current_url,
                    }
                    if link.issue_date:
                        meta["issue_date"] = link.issue_date

                    out.append(
                        UrlRecord(
                            url=abs_url,
                            name=name,
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta=meta,
                        )
                    )
                    if len(out) >= max_total_records:
                        break
                    continue

                is_html_like = ext in {"", ".html", ".htm", ".xhtml"}
                if not is_html_like:
                    continue

                if not parsed.path.startswith(scope_prefix):
                    continue

                if abs_url not in seen_pages:
                    queue.append(abs_url)

        out.sort(key=lambda r: (r.url, (r.meta.get("issue_date") or "")))
        return out
