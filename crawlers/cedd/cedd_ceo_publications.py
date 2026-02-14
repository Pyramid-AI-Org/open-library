from __future__ import annotations

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


@dataclass(frozen=True)
class _Link:
    href: str
    text: str


@dataclass(frozen=True)
class _PwdmCurrentLink:
    href: str
    row_title: str | None
    text: str


class _MainTableParser(HTMLParser):
    """Extract CEO subpage links from the first column of the main listing table."""

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self.links: list[_Link] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_th = False
        self._td_index = -1

        self._in_a = False
        self._current_href: str | None = None
        self._current_link_text_parts: list[str] = []
        self._accessibility_depth = 0

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is not None:
                out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {
            c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()
        }

    def _in_content(self) -> bool:
        return self._content_depth > 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if self._content_depth == 0 and attrs_map.get("id") == self._content_element_id:
            self._content_depth = 1
        elif self._content_depth > 0:
            self._content_depth += 1

        if not self._in_content():
            return

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
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            return

        if t == "td":
            self._td_index += 1
            return

        if t == "a" and not self._in_th and self._td_index == 0:
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_link_text_parts = []
            self._accessibility_depth = 0
            return

        if t == "span" and self._in_a:
            if "accessibility" in self._class_set(attrs_map):
                self._accessibility_depth += 1

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_content():
            return

        if t == "span" and self._in_a and self._accessibility_depth > 0:
            self._accessibility_depth -= 1
            return

        if t == "a" and self._in_a:
            href = clean_text(self._current_href or "")
            text = clean_text("".join(self._current_link_text_parts))
            if href:
                self.links.append(_Link(href=href, text=text))
            self._in_a = False
            self._current_href = None
            self._current_link_text_parts = []
            self._accessibility_depth = 0
            return

        if t == "th" and self._in_th:
            self._in_th = False
            return

        if t == "tr" and self._in_tr:
            self._in_tr = False
            self._in_th = False
            self._td_index = -1
            return

        if self._in_table and self._table_depth > 0:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False

        if self._content_depth > 0:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._in_table or not self._in_tr or self._in_th or not self._in_a:
            return
        if self._accessibility_depth > 0:
            return
        self._current_link_text_parts.append(data)


class _ContentLinkParser(HTMLParser):
    """Extract all anchors within #content."""

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self.links: list[_Link] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._in_a = False
        self._current_href: str | None = None
        self._current_link_text_parts: list[str] = []
        self._accessibility_depth = 0

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is not None:
                out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {
            c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()
        }

    def _in_content(self) -> bool:
        return self._content_depth > 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if self._content_depth == 0 and attrs_map.get("id") == self._content_element_id:
            self._content_depth = 1
        elif self._content_depth > 0:
            self._content_depth += 1

        if not self._in_content():
            return

        if t == "a":
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_link_text_parts = []
            self._accessibility_depth = 0
            return

        if t == "span" and self._in_a:
            if "accessibility" in self._class_set(attrs_map):
                self._accessibility_depth += 1

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_content():
            return

        if t == "span" and self._in_a and self._accessibility_depth > 0:
            self._accessibility_depth -= 1
            return

        if t == "a" and self._in_a:
            href = clean_text(self._current_href or "")
            text = clean_text("".join(self._current_link_text_parts))
            if href:
                self.links.append(_Link(href=href, text=text))
            self._in_a = False
            self._current_href = None
            self._current_link_text_parts = []
            self._accessibility_depth = 0
            return

        if self._content_depth > 0:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._in_a or self._accessibility_depth > 0:
            return
        if not self._in_content():
            return
        self._current_link_text_parts.append(data)


class _PwdmFirstTableCurrentParser(HTMLParser):
    """Extract only 'Current' links from the first table on PWDM subpage."""

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self.links: list[_PwdmCurrentLink] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._table_depth = 0
        self._first_table_seen = False
        self._in_target_table = False

        self._in_tr = False
        self._in_cell = False
        self._cell_is_header = False
        self._cell_index = -1

        self._header_cells: list[str] = []
        self._row_cells: list[str] = []
        self._row_links_by_col: dict[int, list[_Link]] = {}

        self._in_a = False
        self._current_href: str | None = None
        self._current_link_text_parts: list[str] = []
        self._accessibility_depth = 0

        self._cell_text_parts: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is not None:
                out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {
            c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()
        }

    def _in_content(self) -> bool:
        return self._content_depth > 0

    def _current_col_index(self) -> int:
        return self._cell_index if self._cell_index >= 0 else 0

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
            if self._table_depth == 1 and not self._first_table_seen:
                self._first_table_seen = True
                self._in_target_table = True
            return

        if not self._in_target_table:
            return

        if t == "tr":
            self._in_tr = True
            self._cell_index = -1
            self._row_cells = []
            self._row_links_by_col = {}
            return

        if not self._in_tr:
            return

        if t in ("th", "td"):
            self._in_cell = True
            self._cell_is_header = t == "th"
            self._cell_index += 1
            self._cell_text_parts = []
            return

        if t == "a":
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_link_text_parts = []
            self._accessibility_depth = 0
            return

        if t == "span" and self._in_a:
            if "accessibility" in self._class_set(attrs_map):
                self._accessibility_depth += 1

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_content():
            return

        if t == "span" and self._in_a and self._accessibility_depth > 0:
            self._accessibility_depth -= 1
            return

        if t == "a" and self._in_a:
            href = clean_text(self._current_href or "")
            text = clean_text("".join(self._current_link_text_parts))
            if href:
                col = self._current_col_index()
                self._row_links_by_col.setdefault(col, []).append(
                    _Link(href=href, text=text)
                )
            self._in_a = False
            self._current_href = None
            self._current_link_text_parts = []
            self._accessibility_depth = 0
            return

        if not self._in_target_table:
            if t == "table" and self._table_depth > 0:
                self._table_depth -= 1
            if self._content_depth > 0:
                self._content_depth -= 1
            return

        if t in ("th", "td") and self._in_cell:
            cell_text = clean_text("".join(self._cell_text_parts))
            self._row_cells.append(cell_text)
            self._in_cell = False
            self._cell_is_header = False
            self._cell_text_parts = []
            return

        if t == "tr" and self._in_tr:
            if self._row_cells:
                is_header = False
                if self._header_cells:
                    is_header = False
                elif any(clean_text(c) for c in self._row_cells):
                    joined = " ".join(c.lower() for c in self._row_cells)
                    if "current" in joined and "original" in joined:
                        is_header = True

                if is_header:
                    self._header_cells = [c.lower() for c in self._row_cells]
                else:
                    row_title = self._row_cells[0] if self._row_cells else None
                    current_idx: int | None = None
                    for i, hdr in enumerate(self._header_cells):
                        if "current" in hdr:
                            current_idx = i
                            break

                    selected: list[_Link] = []
                    if current_idx is not None:
                        selected = self._row_links_by_col.get(current_idx, [])
                    if not selected:
                        for i in sorted(self._row_links_by_col.keys(), reverse=True):
                            candidate = self._row_links_by_col.get(i, [])
                            if candidate:
                                selected = candidate
                                break

                    for link in selected:
                        self.links.append(
                            _PwdmCurrentLink(
                                href=link.href,
                                row_title=row_title or None,
                                text=link.text,
                            )
                        )

            self._in_tr = False
            self._in_cell = False
            self._cell_is_header = False
            self._cell_index = -1
            self._row_cells = []
            self._row_links_by_col = {}
            return

        if t == "table" and self._table_depth > 0:
            self._table_depth -= 1
            if self._table_depth == 0 and self._in_target_table:
                self._in_target_table = False

        if self._content_depth > 0:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._in_content():
            return

        if self._in_cell:
            self._cell_text_parts.append(data)

        if self._in_a and self._accessibility_depth == 0:
            self._current_link_text_parts.append(data)


class Crawler:
    """Crawl CEDD CEO publications and emit subpage PDFs.

    Special rule:
    - On PWDM subpage, only links in the first table's "Current" column are emitted.
    - For any subpage with no PDFs, emit the subpage URL itself as fallback.
    """

    name = "cedd_ceo_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get("page_url", f"{base_url}/eng/publications/ceo/index.html")
        ).strip()
        scope_prefix = str(cfg.get("scope_prefix", "/eng/publications/ceo/")).strip()
        pwdm_path = str(
            cfg.get("pwdm_path", "/eng/publications/ceo/pwdm/index.html")
        ).strip()
        content_element_id = str(cfg.get("content_element_id", "content")).strip()

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        page_url_canon = canonicalize_url(page_url, encode_spaces=True)
        if not page_url_canon:
            return []

        page_host = urlparse(base_url).netloc.lower()

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if ctx.debug:
            print(f"[{self.name}] Fetch -> {page_url_canon}")

        main_resp = get_with_retries(
            session,
            page_url_canon,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        main_parser = _MainTableParser(content_element_id=content_element_id)
        main_parser.feed(main_resp.text)

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()
        seen_subpages: set[str] = set()

        for sub_link in main_parser.links:
            sub_url = canonicalize_url(
                urljoin(page_url_canon, sub_link.href), encode_spaces=True
            )
            if not sub_url:
                continue

            parsed_sub = urlparse(sub_url)
            if parsed_sub.netloc.lower() != page_host:
                continue

            if not parsed_sub.path.startswith(scope_prefix):
                continue

            if path_ext(sub_url) not in {"", ".html", ".htm", ".xhtml"}:
                continue

            if sub_url in seen_subpages:
                continue
            seen_subpages.add(sub_url)

            if len(out) >= max_total_records:
                break

            if ctx.debug:
                print(f"[{self.name}] Fetch subpage -> {sub_url}")

            sub_resp_text = ""
            fetch_ok = True
            try:
                sub_resp = get_with_retries(
                    session,
                    sub_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                sub_resp_text = sub_resp.text
            except requests.RequestException:
                fetch_ok = False

            emitted_for_subpage = 0

            if fetch_ok:
                discovered_links: list[_Link] = []

                if parsed_sub.path == pwdm_path:
                    pwdm_parser = _PwdmFirstTableCurrentParser(
                        content_element_id=content_element_id
                    )
                    pwdm_parser.feed(sub_resp_text)
                    for lk in pwdm_parser.links:
                        name = clean_text(lk.row_title) or clean_text(lk.text)
                        discovered_links.append(_Link(href=lk.href, text=name))
                else:
                    content_parser = _ContentLinkParser(
                        content_element_id=content_element_id
                    )
                    content_parser.feed(sub_resp_text)
                    discovered_links = list(content_parser.links)

                for doc_link in discovered_links:
                    abs_url = canonicalize_url(
                        urljoin(sub_url, doc_link.href),
                        encode_spaces=True,
                    )
                    if not abs_url:
                        continue
                    parsed_doc = urlparse(abs_url)
                    if parsed_doc.netloc.lower() != page_host:
                        continue
                    if path_ext(abs_url) != ".pdf":
                        continue
                    if abs_url in seen_urls:
                        continue

                    seen_urls.add(abs_url)
                    emitted_for_subpage += 1

                    title = clean_text(doc_link.text) or infer_name_from_link(
                        doc_link.text, abs_url
                    )

                    out.append(
                        UrlRecord(
                            url=abs_url,
                            name=title,
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta={
                                "title": title,
                                "discovered_from": sub_url,
                            },
                        )
                    )

                    if len(out) >= max_total_records:
                        break

            if len(out) >= max_total_records:
                break

            if emitted_for_subpage == 0 and sub_url not in seen_urls:
                seen_urls.add(sub_url)
                fallback_title = clean_text(sub_link.text) or infer_name_from_link(
                    sub_link.text, sub_url
                )

                out.append(
                    UrlRecord(
                        url=sub_url,
                        name=fallback_title,
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "title": fallback_title,
                            "discovered_from": page_url_canon,
                            "fallback": "subpage_without_pdf",
                        },
                    )
                )

        out.sort(key=lambda r: (r.url, clean_text(str(r.meta.get("title") or ""))))
        return out
