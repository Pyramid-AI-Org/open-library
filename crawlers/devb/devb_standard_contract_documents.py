from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin


STANDARD_CONTRACT_DOCS_PREFIX = (
    "/en/publications_and_press_releases/publications/standard_contract_documents/"
)


_ALLOWED_DOC_EXTS = {".pdf", ".doc", ".docx"}


def _normalize_ws(value: str | None) -> str | None:
    s = " ".join((value or "").split()).strip()
    return s or None


def _is_allowed_doc_url(url: str) -> bool:
    lower = (url or "").lower()
    for sep in ("#", "?"):
        if sep in lower:
            lower = lower.split(sep, 1)[0]
    return any(lower.endswith(ext) for ext in _ALLOWED_DOC_EXTS)


def _looks_like_html_page(url: str) -> bool:
    u = (url or "").lower()
    if u.endswith("/"):
        return True
    if u.endswith(".html"):
        return True
    if u.endswith(".htm"):
        return True
    if u.endswith(".php"):
        return True
    if "?" in u:
        # Some DEVb pages use query flags (e.g., print=1).
        return True
    return False


@dataclass(frozen=True)
class StandardContractDocHit:
    url: str
    title: str | None
    issue_date_raw: str | None
    meta: dict[str, str]


@dataclass(frozen=True)
class _Cell:
    text: str | None
    hrefs: tuple[str, ...]
    colspan: int


class _ArticleListTableParser(HTMLParser):
    def __init__(self, *, base_url: str, element_id: str) -> None:
        super().__init__()
        self._base_url = base_url
        self._element_id = element_id

        self.doc_hits: list[StandardContractDocHit] = []
        self.page_links: set[str] = set()

        self._current_section: str | None = None

        # If element_id is falsy, parse the entire HTML document.
        self._target_depth = 1 if not self._element_id else 0

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_th = False
        self._in_td = False

        self._current_row_cells: list[_Cell] = []

        self._current_text_parts: list[str] = []
        self._current_hrefs: list[str] = []
        self._current_colspan = 1

    def _attrs_to_dict(self, attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if self._target_depth == 0 and self._element_id and attrs_map.get("id") == self._element_id:
            self._target_depth = 1
        elif self._target_depth > 0:
            self._target_depth += 1

        if self._target_depth <= 0:
            return

        if not self._in_table and t == "table":
            cls = attrs_map.get("class", "")
            classes = set(cls.split())
            if "articlelistpage" in classes:
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
            self._current_row_cells = []
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            return

        if t == "td":
            self._in_td = True
            self._current_text_parts = []
            self._current_hrefs = []
            try:
                self._current_colspan = int(attrs_map.get("colspan", "1") or "1")
            except ValueError:
                self._current_colspan = 1
            return

        if t == "a" and self._in_td:
            href = attrs_map.get("href")
            if href:
                self._current_hrefs.append(urljoin(self._base_url, href))
            return

        if t == "br" and self._in_td:
            self._current_text_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if self._target_depth > 0:
            self._target_depth -= 1
            if self._target_depth == 0:
                self._in_table = False
                self._table_depth = 0
                self._in_tr = False
                self._in_th = False
                self._in_td = False
                self._current_row_cells = []
                return

        if not self._in_table:
            return

        self._table_depth -= 1
        if self._table_depth == 0:
            self._in_table = False
            self._in_tr = False
            self._in_th = False
            self._in_td = False
            self._current_row_cells = []
            return

        if t == "th":
            self._in_th = False
            return

        if not self._in_tr:
            return

        if t == "td" and not self._in_th:
            self._in_td = False
            text = _normalize_ws("".join(self._current_text_parts))
            hrefs = tuple(self._current_hrefs)
            colspan = self._current_colspan if self._current_colspan > 0 else 1

            self._current_row_cells.append(_Cell(text=text, hrefs=hrefs, colspan=colspan))

            self._current_text_parts = []
            self._current_hrefs = []
            self._current_colspan = 1
            return

        if t == "tr":
            self._in_tr = False

            # Ignore header rows.
            if not self._current_row_cells:
                return

            self._process_row(self._current_row_cells)
            self._current_row_cells = []

    def handle_data(self, data: str) -> None:
        if self._in_td and self._target_depth > 0:
            self._current_text_parts.append(data)

    def _process_row(self, cells: list[_Cell]) -> None:
        # Collect in-scope page links (folder rows etc.).
        for c in cells:
            for href in c.hrefs:
                if (
                    STANDARD_CONTRACT_DOCS_PREFIX in href
                    and _looks_like_html_page(href)
                    and not _is_allowed_doc_url(href)
                ):
                    self.page_links.add(href)

        # Detect section header rows (admin procedures page style).
        row_text = _normalize_ws(" ".join([c.text or "" for c in cells]))
        any_hrefs = any(c.hrefs for c in cells)
        any_doc_hrefs = any(_is_allowed_doc_url(h) for c in cells for h in c.hrefs)

        if not any_hrefs and row_text and any(c.colspan >= 2 for c in cells):
            self._current_section = row_text
            return

        # 1) Practice notes tables (Clean / Track change) - only emit from clean column.
        if len(cells) in (4, 5):
            if len(cells) == 4:
                item_idx, clean_idx, date_idx = 0, 1, 3
            else:
                item_idx, clean_idx, date_idx = 1, 2, 4

            title = cells[item_idx].text
            issue_date_raw = cells[date_idx].text

            clean_hrefs = [h for h in cells[clean_idx].hrefs if _is_allowed_doc_url(h)]
            for href in clean_hrefs:
                self.doc_hits.append(
                    StandardContractDocHit(
                        url=href,
                        title=title,
                        issue_date_raw=issue_date_raw,
                        meta={
                            "kind": "practice_notes",
                            "variant": "clean",
                        },
                    )
                )
            if clean_hrefs:
                return

        # 2) Administrative procedures content page (section + item rows)
        if len(cells) >= 4:
            first = (cells[0].text or "").strip()
            title_candidate = cells[1].text
            doc_hrefs = [h for h in cells[2].hrefs if _is_allowed_doc_url(h)]
            issue_date_raw = cells[3].text

            if (first in {"", "-", "–"} or first.isdigit()) and title_candidate and (
                doc_hrefs or (issue_date_raw and any_doc_hrefs)
            ):
                for href in doc_hrefs:
                    meta = {"kind": "administrative_procedures"}
                    if self._current_section:
                        meta["section"] = self._current_section

                    self.doc_hits.append(
                        StandardContractDocHit(
                            url=href,
                            title=title_candidate,
                            issue_date_raw=issue_date_raw,
                            meta=meta,
                        )
                    )
                if doc_hrefs:
                    return

        # 3) Standard contract docs index-style (number | title | file links)
        if len(cells) >= 3:
            first = (cells[0].text or "").strip()
            title = cells[1].text
            doc_hrefs: list[str] = []
            for c in cells[2:]:
                for h in c.hrefs:
                    if _is_allowed_doc_url(h):
                        doc_hrefs.append(h)

            if (first.isdigit() or not first) and title and doc_hrefs:
                for href in doc_hrefs:
                    self.doc_hits.append(
                        StandardContractDocHit(
                            url=href,
                            title=title,
                            issue_date_raw=None,
                            meta={"kind": "index"},
                        )
                    )
                return


def parse_standard_contract_documents_page(
    html: str, *, base_url: str, content_element_id: str = "content"
) -> tuple[list[StandardContractDocHit], list[str]]:
    """Parse DEVb standard contract documents pages.

    Returns:
      - doc hits: url + title + raw issue date (if present)
      - page links: in-scope HTML pages to continue crawling

    Notes:
      - Emits ALL docs found, but for practice-notes tables only from the "Clean" column.
      - Filters to .pdf/.doc/.docx only; ignores zip and other assets.
      - Does not canonicalize or dedupe; caller should do that.
    """

    parser = _ArticleListTableParser(base_url=base_url, element_id=content_element_id)
    parser.feed(html or "")

    if not parser.doc_hits and not parser.page_links:
        # Fallback for unexpected layouts / local fixtures.
        parser = _ArticleListTableParser(base_url=base_url, element_id="")
        parser.feed(html or "")

    docs: list[StandardContractDocHit] = []
    for h in parser.doc_hits:
        if _is_allowed_doc_url(h.url):
            docs.append(h)

    pages = [p for p in parser.page_links if STANDARD_CONTRACT_DOCS_PREFIX in p]
    return docs, pages
