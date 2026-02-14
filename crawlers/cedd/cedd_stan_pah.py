from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
)


@dataclass(frozen=True)
class _PahRow:
    item: str | None
    first_issue_href: str | None
    clean_href: str | None
    issue_date: str | None


class _StanPahParser(HTMLParser):
    """Parse stan-pah main table.

    Required headers:
      - Item
      - First Issue
      - Current Issue - Clean version
      - Current Issue - Issue Date
    """

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self.rows: list[_PahRow] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._table_depth = 0
        self._active_table = False

        self._in_tr = False
        self._in_th = False
        self._in_td = False

        self._header_cells: list[str] = []
        self._header_index_by_name: dict[str, int] = {}

        self._current_row_cells: list[str] = []
        self._current_row_hrefs: dict[int, str] = {}
        self._current_cell_text_parts: list[str] = []

        self._cell_index = -1

        self._in_a = False
        self._current_href: str | None = None
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
    def _normalize_header(text: str) -> str:
        return clean_text(text).lower()

    def _in_content(self) -> bool:
        return self._content_depth > 0

    def _is_target_header(self) -> bool:
        required = {
            "item",
            "first issue",
            "current issue - clean version",
            "current issue - issue date",
        }
        return required.issubset(set(self._header_index_by_name.keys()))

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
            if self._table_depth == 1:
                self._active_table = False
                self._header_cells = []
                self._header_index_by_name = {}
            return

        if self._table_depth <= 0:
            return

        if t == "tr":
            self._in_tr = True
            self._in_th = False
            self._in_td = False
            self._current_row_cells = []
            self._current_row_hrefs = {}
            self._cell_index = -1
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            self._in_td = False
            self._cell_index += 1
            self._current_cell_text_parts = []
            return

        if t == "td":
            self._in_td = True
            self._in_th = False
            self._cell_index += 1
            self._current_cell_text_parts = []
            return

        if t == "a" and self._in_td:
            self._in_a = True
            self._current_href = attrs_map.get("href")
            return

        if t == "span" and self._in_a:
            cls = clean_text(attrs_map.get("class", "")).lower()
            if "accessibility" in cls.split():
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
            if href and self._in_td and self._cell_index >= 0:
                if self._cell_index not in self._current_row_hrefs:
                    self._current_row_hrefs[self._cell_index] = href
            self._in_a = False
            self._current_href = None
            self._in_accessibility_span = False
            return

        if t == "th" and self._in_th:
            self._in_th = False
            self._current_row_cells.append(clean_text("".join(self._current_cell_text_parts)))
            self._current_cell_text_parts = []
            return

        if t == "td" and self._in_td:
            self._in_td = False
            self._current_row_cells.append(clean_text("".join(self._current_cell_text_parts)))
            self._current_cell_text_parts = []
            return

        if t == "tr" and self._in_tr:
            if self._current_row_cells:
                # Header row detection.
                if not self._header_index_by_name:
                    self._header_cells = self._current_row_cells[:]
                    for idx, h in enumerate(self._header_cells):
                        key = self._normalize_header(h)
                        if key:
                            self._header_index_by_name[key] = idx
                    self._active_table = self._is_target_header()
                elif self._active_table:
                    idx_item = self._header_index_by_name.get("item")
                    idx_first = self._header_index_by_name.get("first issue")
                    idx_clean = self._header_index_by_name.get(
                        "current issue - clean version"
                    )
                    idx_issue_date = self._header_index_by_name.get(
                        "current issue - issue date"
                    )

                    if (
                        idx_item is not None
                        and idx_first is not None
                        and idx_clean is not None
                        and idx_issue_date is not None
                    ):
                        item = (
                            self._current_row_cells[idx_item]
                            if idx_item < len(self._current_row_cells)
                            else ""
                        )
                        issue_date = (
                            self._current_row_cells[idx_issue_date]
                            if idx_issue_date < len(self._current_row_cells)
                            else ""
                        )
                        first_href = self._current_row_hrefs.get(idx_first)
                        clean_href = self._current_row_hrefs.get(idx_clean)

                        if item and (clean_href or first_href):
                            self.rows.append(
                                _PahRow(
                                    item=item or None,
                                    first_issue_href=first_href,
                                    clean_href=clean_href,
                                    issue_date=issue_date or None,
                                )
                            )

            self._in_tr = False
            self._current_row_cells = []
            self._current_row_hrefs = {}
            self._cell_index = -1
            return

        if t == "table" and self._table_depth > 0:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._active_table = False
                self._header_cells = []
                self._header_index_by_name = {}

        if self._content_depth > 0:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._in_content():
            return
        if self._in_accessibility_span:
            return
        if self._in_th or self._in_td:
            self._current_cell_text_parts.append(data)


def crawl_stan_pah_subpage(
    *,
    session: requests.Session,
    page_url: str,
    base_url: str,
    discovered_at_utc: str,
    source: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
    content_element_id: str = "content",
) -> list[UrlRecord]:
    resp = get_with_retries(
        session,
        page_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )

    parser = _StanPahParser(content_element_id=content_element_id)
    parser.feed(resp.text)

    out: list[UrlRecord] = []
    seen: set[str] = set()

    for row in parser.rows:
        chosen_href = row.clean_href or row.first_issue_href
        if not chosen_href:
            continue

        abs_url = canonicalize_url(urljoin(page_url, chosen_href), encode_spaces=True)
        if not abs_url:
            continue
        if not abs_url.startswith(base_url.rstrip("/") + "/"):
            continue
        if path_ext(abs_url) != ".pdf":
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)

        name = clean_text(row.item or "") or infer_name_from_link(row.item, abs_url)
        meta: dict[str, str | None] = {
            "discovered_from": page_url,
            "issue_date": row.issue_date,
        }

        out.append(
            UrlRecord(
                url=abs_url,
                name=name,
                discovered_at_utc=discovered_at_utc,
                source=source,
                meta=meta,
            )
        )

    out.sort(key=lambda r: (r.url, (r.meta.get("issue_date") or "")))
    return out
