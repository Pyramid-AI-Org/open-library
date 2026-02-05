from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


STANDARD_CONSULTANCY_DOCS_PREFIX = (
    "/en/publications_and_press_releases/publications/standard_consultancy_document/"
)


_ALLOWED_DOC_EXTS = {".pdf", ".doc", ".docx"}


def _normalize_ws(value: str | None) -> str | None:
    s = " ".join((value or "").split()).strip()
    return s or None


def _strip_query_fragment(url: str) -> str:
    s = url or ""
    for sep in ("#", "?"):
        if sep in s:
            s = s.split(sep, 1)[0]
    return s


def _doc_ext(url: str) -> str:
    path = urlparse(_strip_query_fragment(url)).path.lower()
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


def _is_allowed_doc_url(url: str) -> bool:
    return _doc_ext(url) in _ALLOWED_DOC_EXTS


def _looks_like_html_page(url: str) -> bool:
    u = (url or "").lower()
    if u.endswith("/"):
        return True
    if u.endswith(".html") or u.endswith(".htm") or u.endswith(".php"):
        return True
    if "?" in u:
        return True
    return False


def _pick_preferred_current(hrefs: list[str]) -> str | None:
    allowed = [h for h in hrefs if _is_allowed_doc_url(h)]
    if not allowed:
        return None

    # Prefer PDF over Word.
    for ext in (".pdf", ".docx", ".doc"):
        for h in allowed:
            if _doc_ext(h) == ext:
                return h

    return allowed[0]


@dataclass(frozen=True)
class StandardConsultancyDocHit:
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

        self.doc_hits: list[StandardConsultancyDocHit] = []
        self.page_links: set[str] = set()

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
                self._reset_all()
                return

        if not self._in_table:
            return

        self._table_depth -= 1
        if self._table_depth == 0:
            self._reset_all()
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
            if not self._current_row_cells:
                return
            self._process_row(self._current_row_cells)
            self._current_row_cells = []

    def _reset_all(self) -> None:
        self._in_table = False
        self._table_depth = 0
        self._in_tr = False
        self._in_th = False
        self._in_td = False
        self._current_row_cells = []

    def handle_data(self, data: str) -> None:
        if self._in_td and self._target_depth > 0:
            self._current_text_parts.append(data)

    def _process_row(self, cells: list[_Cell]) -> None:
        # Collect in-scope page links (folder rows etc.).
        for c in cells:
            for href in c.hrefs:
                if (
                    STANDARD_CONSULTANCY_DOCS_PREFIX in href
                    and _looks_like_html_page(href)
                    and not _is_allowed_doc_url(href)
                ):
                    self.page_links.add(href)

        # Subpages: (no.) | Item | Current Version | Highlighted | Issue Date
        if len(cells) in (4, 5):
            if len(cells) == 5:
                item_idx, current_idx, date_idx = 1, 2, 4
            else:
                item_idx, current_idx, date_idx = 0, 1, 3

            title = cells[item_idx].text
            issue_date_raw = cells[date_idx].text

            candidate = _pick_preferred_current(list(cells[current_idx].hrefs))
            if title and candidate:
                self.doc_hits.append(
                    StandardConsultancyDocHit(
                        url=candidate,
                        title=title,
                        issue_date_raw=issue_date_raw,
                        meta={"kind": "table", "variant": "current"},
                    )
                )
                return

        # Index page: number | Document | Filename
        if len(cells) >= 3:
            title = cells[1].text
            candidate = _pick_preferred_current(list(cells[2].hrefs))
            if title and candidate:
                self.doc_hits.append(
                    StandardConsultancyDocHit(
                        url=candidate,
                        title=title,
                        issue_date_raw=None,
                        meta={"kind": "index", "variant": "current"},
                    )
                )
                return


def parse_standard_consultancy_documents_page(
    html: str, *, base_url: str, content_element_id: str = "content"
) -> tuple[list[StandardConsultancyDocHit], list[str]]:
    """Parse DEVb standard consultancy documents pages.

    Rules:
      - Title always comes from the Document/Item column (not from the filename).
      - Ignore offsite links (caller filters by netloc).
      - Emit only the *current version* document URL per row.
      - If both PDF and Word exist for current version, choose PDF.
      - Filters to .pdf/.doc/.docx only; ignores zip and other assets.
      - Does not canonicalize or dedupe; caller should do that.
    """

    parser = _ArticleListTableParser(base_url=base_url, element_id=content_element_id)
    parser.feed(html or "")

    if not parser.doc_hits and not parser.page_links:
        # Fallback for unexpected layouts / local fixtures.
        parser = _ArticleListTableParser(base_url=base_url, element_id="")
        parser.feed(html or "")

    docs: list[StandardConsultancyDocHit] = []
    for h in parser.doc_hits:
        if _is_allowed_doc_url(h.url):
            docs.append(h)

    pages = [p for p in parser.page_links if STANDARD_CONSULTANCY_DOCS_PREFIX in p]
    return docs, pages
