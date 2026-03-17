from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


WORKS_DIGEST_PREFIX = "/en/publications_and_press_releases/publications/works_digest/"


_ALLOWED_DOC_EXTS = {".pdf", ".doc", ".docx"}


_MONTH_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


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


def _issue_info_from_path(url: str) -> tuple[str | None, str | None, str | None]:
    path = (urlparse(url).path or "").lower()
    m = re.search(
        r"/issue_(?P<num>\d+)_(?P<month>[a-z]+)_(?P<year>\d{4})(?:/|$)",
        path,
    )
    if not m:
        return None, None, None

    issue_number = m.group("num")
    month_name = m.group("month")
    year = m.group("year")

    month_num = _MONTH_TO_NUM.get(month_name)
    if not month_num:
        return issue_number, None, None

    publish_date = f"{year}-{month_num:02d}-01"
    issue_key = f"{issue_number}-{year}-{month_num:02d}"
    return issue_number, publish_date, issue_key


def _issue_info_from_text(text: str | None) -> tuple[str | None, str | None, str | None]:
    t = (text or "").strip()
    if not t:
        return None, None, None

    m = re.search(
        r"ISSUE\s*NO\.?\s*(?P<num>\d+)\s*,\s*(?P<month>[A-Za-z]+)\s*,\s*(?P<year>\d{4})",
        t,
        flags=re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r"Issue\s*(?P<num>\d+)\s*,\s*(?P<month>[A-Za-z]+)\s*(?P<year>\d{4})",
            t,
            flags=re.IGNORECASE,
        )
    if not m:
        return None, None, None

    issue_number = m.group("num")
    month_name = m.group("month").lower()
    year = m.group("year")

    month_num = _MONTH_TO_NUM.get(month_name)
    if not month_num:
        return issue_number, None, None

    publish_date = f"{year}-{month_num:02d}-01"
    issue_key = f"{issue_number}-{year}-{month_num:02d}"
    return issue_number, publish_date, issue_key


def looks_like_full_text(title: str | None, url: str) -> bool:
    title_text = (title or "").strip().lower()
    if "full text" in title_text:
        return True

    path = (urlparse(url).path or "").lower()
    filename = path.rsplit("/", 1)[-1]
    if filename in {"full.pdf", "full.doc", "full.docx"}:
        return True

    return False


@dataclass(frozen=True)
class WorksDigestContext:
    issue_number: str | None
    publish_date_raw: str | None
    issue_key: str | None


@dataclass(frozen=True)
class WorksDigestHit:
    url: str
    title: str | None
    meta: dict[str, str]


class _WorksDigestParser(HTMLParser):
    def __init__(self, *, base_url: str, element_id: str) -> None:
        super().__init__()
        self._base_url = base_url
        self._element_id = element_id

        # If element_id is falsy, parse the entire HTML document.
        self._target_depth = 1 if not self._element_id else 0

        self._in_h1 = False
        self._h1_text_parts: list[str] = []

        self.doc_hits: list[WorksDigestHit] = []
        self.page_links: set[str] = set()

        self._current_href: str | None = None
        self._current_text_parts: list[str] = []

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

        if (
            self._target_depth == 0
            and self._element_id
            and attrs_map.get("id") == self._element_id
        ):
            self._target_depth = 1
        elif self._target_depth > 0:
            self._target_depth += 1

        if self._target_depth <= 0:
            return

        if t == "h1":
            self._in_h1 = True
            self._h1_text_parts = []
            return

        if t == "a":
            href = attrs_map.get("href")
            if href:
                self._current_href = urljoin(self._base_url, href)
                self._current_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if self._target_depth > 0:
            self._target_depth -= 1
            if self._target_depth == 0:
                self._current_href = None
                self._current_text_parts = []
                self._in_h1 = False
                self._h1_text_parts = []
                return

        if self._target_depth <= 0:
            return

        if t == "h1" and self._in_h1:
            self._in_h1 = False
            return

        if t == "a" and self._current_href:
            href = self._current_href
            text = _normalize_ws("".join(self._current_text_parts))

            if _is_allowed_doc_url(href):
                self.doc_hits.append(WorksDigestHit(url=href, title=text, meta={}))
            elif WORKS_DIGEST_PREFIX in href and _looks_like_html_page(href):
                self.page_links.add(href)

            self._current_href = None
            self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._target_depth <= 0:
            return

        if self._in_h1:
            self._h1_text_parts.append(data)

        if self._current_href:
            self._current_text_parts.append(data)

    def issue_heading_text(self) -> str | None:
        return _normalize_ws("".join(self._h1_text_parts))


def parse_works_digest_page(
    html: str,
    *,
    page_url: str,
    content_element_id: str = "content",
) -> tuple[list[WorksDigestHit], list[str], WorksDigestContext]:
    """Parse DEVb Works Digest pages.

    Returns docs + in-scope page links and issue context, where issue date is inferred
    from URL pattern first, then heading text fallback.
    """

    parser = _WorksDigestParser(base_url=page_url, element_id=content_element_id)
    parser.feed(html or "")

    if not parser.doc_hits and not parser.page_links:
        parser = _WorksDigestParser(base_url=page_url, element_id="")
        parser.feed(html or "")

    issue_number, publish_date_raw, issue_key = _issue_info_from_path(page_url)

    if not (issue_number and publish_date_raw and issue_key):
        h_issue_num, h_publish_date, h_issue_key = _issue_info_from_text(
            parser.issue_heading_text()
        )
        issue_number = issue_number or h_issue_num
        publish_date_raw = publish_date_raw or h_publish_date
        issue_key = issue_key or h_issue_key

    ctx = WorksDigestContext(
        issue_number=issue_number,
        publish_date_raw=publish_date_raw,
        issue_key=issue_key,
    )

    docs = [h for h in parser.doc_hits if _is_allowed_doc_url(h.url)]
    pages = [p for p in parser.page_links if WORKS_DIGEST_PREFIX in p]
    return docs, pages, ctx
