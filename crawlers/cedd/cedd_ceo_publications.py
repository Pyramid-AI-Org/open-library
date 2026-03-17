from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import re
from typing import Callable
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
class _DiscoveredDoc:
    href: str
    name: str | None
    publish_date: str | None = None


@dataclass(frozen=True)
class _TableRow:
    cells: list[str]
    links_by_col: dict[int, list[_Link]]
    is_header: bool


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


class _FirstTableRowsParser(HTMLParser):
    """Extract rows (cells + links) from the first table within #content."""

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self.rows: list[_TableRow] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._table_depth = 0
        self._first_table_seen = False
        self._in_target_table = False

        self._in_tr = False
        self._in_cell = False
        self._cell_index = -1

        self._row_cells: list[str] = []
        self._row_links_by_col: dict[int, list[_Link]] = {}
        self._row_has_th = False

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
            self._row_has_th = False
            return

        if not self._in_tr:
            return

        if t in ("th", "td"):
            self._in_cell = True
            self._cell_index += 1
            self._cell_text_parts = []
            if t == "th":
                self._row_has_th = True
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
            self._cell_text_parts = []
            return

        if t == "tr" and self._in_tr:
            if self._row_cells:
                self.rows.append(
                    _TableRow(
                        cells=list(self._row_cells),
                        links_by_col={
                            col: list(links)
                            for col, links in self._row_links_by_col.items()
                        },
                        is_header=self._row_has_th,
                    )
                )
            self._in_tr = False
            self._in_cell = False
            self._cell_index = -1
            self._row_cells = []
            self._row_links_by_col = {}
            self._row_has_th = False
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


def _find_header_index(headers: list[str], *, terms: tuple[str, ...]) -> int | None:
    for i, header in enumerate(headers):
        normalized = clean_text(header).lower()
        if any(term in normalized for term in terms):
            return i
    return None


def _publish_date_from_no_cell(no_cell: str | None) -> str | None:
    text = clean_text(no_cell or "")
    if not text:
        return None

    match = re.search(r"/\s*(\d{4})\b", text)
    if not match:
        match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if not match:
        return None

    return f"{match.group(1)}-01-01"


def _extract_pwdm_docs(rows: list[_TableRow]) -> list[_DiscoveredDoc]:
    docs: list[_DiscoveredDoc] = []
    headers: list[str] = []

    for row in rows:
        if row.is_header and not headers:
            if any(clean_text(cell) for cell in row.cells):
                headers = [clean_text(cell).lower() for cell in row.cells]
            continue

        if not row.cells:
            continue

        current_idx = _find_header_index(headers, terms=("current",))
        name_idx = _find_header_index(
            headers,
            terms=("name/item", "name / item", "name"),
        )

        selected_links: list[_Link] = []
        if current_idx is not None:
            selected_links = list(row.links_by_col.get(current_idx, []))
        if not selected_links:
            for col in sorted(row.links_by_col.keys(), reverse=True):
                candidates = row.links_by_col.get(col, [])
                if candidates:
                    selected_links = list(candidates)
                    break

        if not selected_links:
            continue

        row_name = ""
        if name_idx is not None and 0 <= name_idx < len(row.cells):
            row_name = clean_text(row.cells[name_idx])
        if not row_name and row.cells:
            row_name = clean_text(row.cells[0])

        for link in selected_links:
            docs.append(
                _DiscoveredDoc(
                    href=link.href,
                    name=row_name or clean_text(link.text) or None,
                )
            )

    return docs


def _extract_title_no_docs(rows: list[_TableRow]) -> list[_DiscoveredDoc]:
    docs: list[_DiscoveredDoc] = []
    headers: list[str] = []

    for row in rows:
        if row.is_header and not headers:
            if any(clean_text(cell) for cell in row.cells):
                headers = [clean_text(cell).lower() for cell in row.cells]
            continue

        if not row.cells or not row.links_by_col:
            continue

        title_idx = _find_header_index(headers, terms=("title",))
        no_idx = _find_header_index(headers, terms=("no.", "no"))

        row_title = ""
        if title_idx is not None and 0 <= title_idx < len(row.cells):
            row_title = clean_text(row.cells[title_idx])
        if not row_title and row.cells:
            row_title = clean_text(row.cells[0])

        no_value = (
            row.cells[no_idx] if no_idx is not None and no_idx < len(row.cells) else ""
        )
        publish_date = _publish_date_from_no_cell(no_value)

        for col in sorted(row.links_by_col.keys()):
            for link in row.links_by_col.get(col, []):
                docs.append(
                    _DiscoveredDoc(
                        href=link.href,
                        name=row_title or clean_text(link.text) or None,
                        publish_date=publish_date,
                    )
                )

    return docs


def _extract_first_table_docs(
    html: str,
    *,
    content_element_id: str,
    extractor: Callable[[list[_TableRow]], list[_DiscoveredDoc]],
) -> list[_DiscoveredDoc]:
    rows_parser = _FirstTableRowsParser(content_element_id=content_element_id)
    rows_parser.feed(html)
    return extractor(rows_parser.rows)


class Crawler:
    """Crawl CEDD CEO publications and emit subpage PDFs.

    Special rule:
    - On PWDM subpage, only links in the first table's "Current" column are emitted.
    - For any subpage with no PDFs, emit the subpage URL itself as fallback.
    """

    name = "cedd_ceo_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get("page_url", f"{base_url}/eng/publications/ceo/index.html")
        ).strip()
        scope_prefix = str(cfg.get("scope_prefix", "/eng/publications/ceo/")).strip()
        pwdm_path = str(
            cfg.get("pwdm_path", "/eng/publications/ceo/pwdm/index.html")
        ).strip()
        title_no_paths_cfg = cfg.get(
            "title_no_paths",
            [
                "/eng/publications/ceo/d-guidelines-frp/index.html",
                "/eng/publications/ceo/guidelines-design-im-eco-as/index.html",
            ],
        )
        title_no_paths = {
            clean_text(str(path)).strip()
            for path in title_no_paths_cfg
            if clean_text(str(path)).strip()
        }
        content_element_id = str(cfg.get("content_element_id", "content")).strip()

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
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
                discovered_docs: list[_DiscoveredDoc] = []

                if parsed_sub.path == pwdm_path:
                    discovered_docs = _extract_first_table_docs(
                        sub_resp_text,
                        content_element_id=content_element_id,
                        extractor=_extract_pwdm_docs,
                    )
                elif parsed_sub.path in title_no_paths:
                    discovered_docs = _extract_first_table_docs(
                        sub_resp_text,
                        content_element_id=content_element_id,
                        extractor=_extract_title_no_docs,
                    )
                else:
                    content_parser = _ContentLinkParser(
                        content_element_id=content_element_id
                    )
                    content_parser.feed(sub_resp_text)
                    discovered_docs = [
                        _DiscoveredDoc(href=link.href, name=link.text)
                        for link in content_parser.links
                    ]

                for doc_link in discovered_docs:
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

                    title = clean_text(doc_link.name) or infer_name_from_link(
                        doc_link.name, abs_url
                    )

                    out.append(
                        ctx.make_record(
                            url=abs_url,
                            name=title,
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta={"discovered_from": sub_url},
                            publish_date=doc_link.publish_date,
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
                    ctx.make_record(
                        url=sub_url,
                        name=fallback_title,
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={"discovered_from": page_url_canon},
                    )
                )

        out.sort(key=lambda r: (r.url, clean_text(r.name or "")))
        return out
