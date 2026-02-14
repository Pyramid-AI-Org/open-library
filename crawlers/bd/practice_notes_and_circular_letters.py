from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date
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
    sleep_seconds,
)


_ALLOWED_DOC_EXTS = {".pdf"}


_clean_text = clean_text
_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries
_canonicalize_url = canonicalize_url
_path_ext = path_ext


@dataclass(frozen=True)
class _DocHit:
    url: str
    name: str
    ref_no: str | None
    date_str: str | None
    tab: str | None
    year: int | None


class _PracticeNoteParser(HTMLParser):
    """
    Parses table structures like:
    [Ref No/Link] | [Title + more details] | [Date]

    Handles tabbed content (PNAP) by tracking div ids (pane-A, pane-B, etc.).
    """

    def __init__(self, *, base_url: str, tab_map: dict[str, str] | None = None) -> None:
        super().__init__()
        self._base_url = base_url
        self._tab_map = tab_map or {}  # id -> tab name

        self.hits: list[_DocHit] = []

        self._in_table = False
        self._table_depth = 0
        self._in_tbody = False
        self._in_tr = False
        self._td_index = -1

        self._current_tab: str | None = None
        self._tab_depth = 0

        self._current_ref_no: str | None = None
        self._current_main_link: str | None = None
        self._current_title_parts: list[str] = []
        self._current_date: str | None = None

        self._capture_text = False
        self._text_parts: list[str] = []
        self._in_a = False
        self._current_href: str | None = None

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {k.lower(): v for k, v in attrs if v is not None}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        # Tab handling
        if t == "div":
            div_id = attrs_map.get("id")
            if div_id and div_id in self._tab_map:
                self._current_tab = self._tab_map[div_id]
                self._tab_depth = 1
            elif self._tab_depth > 0:
                self._tab_depth += 1
            return

        if t == "table":
            if self._table_depth == 0:
                self._in_table = True
            self._table_depth += 1
            return

        if not self._in_table:
            return

        if t == "tbody":
            self._in_tbody = True
            return

        if t == "tr" and self._in_tbody:
            self._in_tr = True
            self._td_index = -1
            self._current_ref_no = None
            self._current_main_link = None
            self._current_title_parts = []
            self._current_date = None
            return

        if t == "td" and self._in_tr:
            self._td_index += 1
            self._capture_text = True
            self._text_parts = []
            return

        if t == "a" and self._in_tr:
            self._in_a = True
            self._current_href = attrs_map.get("href")
            # If we are in the title column (index 1), we generally want to ignore "More details"
            # or nested links in the hidden div.
            # However, for Ref No column (index 0), the link is the one we want.

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._tab_depth > 0:
            self._tab_depth -= 1
            if self._tab_depth == 0:
                self._current_tab = None
            return

        if t == "table":
            if self._table_depth > 0:
                self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
                self._in_tbody = False
            return

        if not self._in_table:
            return

        if t == "tbody":
            self._in_tbody = False
            return

        if t == "tr" and self._in_tr:
            if self._current_main_link and self._current_title_parts:
                can = _canonicalize_url(
                    urljoin(self._base_url, self._current_main_link)
                )
                if can and _path_ext(can) in _ALLOWED_DOC_EXTS:
                    name_str = _clean_text(" ".join(self._current_title_parts))
                    ref_str = _clean_text(self._current_ref_no or "")
                    date_str = _clean_text(self._current_date or "")

                    if ref_str and ref_str not in name_str:
                        full_name = f"{ref_str} - {name_str}"
                    else:
                        full_name = name_str

                    self.hits.append(
                        _DocHit(
                            url=can,
                            name=full_name,
                            ref_no=ref_str or None,
                            date_str=date_str or None,
                            tab=self._current_tab,
                            year=None,
                        )
                    )

            self._in_tr = False
            return

        if t == "td" and self._in_tr:
            text = _clean_text(" ".join(self._text_parts))
            self._capture_text = False
            self._text_parts = []

            # Col 0: Ref No + Link
            if self._td_index == 0:
                self._current_ref_no = text
                # Logic: The link in this column is the document.
                # If handle_starttag/data captured text, we used it for ref no.
                # The href was captured in handle_starttag/endtag for 'a' below?
                # No, we need to capture href inside this TD.
                # Ideally, we should have captured the href in handle_starttag.
                pass

            # Col 1: Title
            elif self._td_index == 1:
                # We want the text of this cell, but excluding "More details" etc.
                # The text accumulated in _text_parts includes all text.
                # We need to be careful. The "More details" is usually an anchor text?
                # Let's rely on accumulated text but we might need to filter.
                pass

            # Col 2: Date
            elif self._td_index == 2:
                self._current_date = text

            return

        if t == "a" and self._in_tr:
            self._in_a = False

            link_text = _clean_text(" ".join(self._text_parts))
            # If we are inside an <a>, the text is link text.
            # _text_parts currently collects all text in TD.
            # This logic is a bit mixed. Let's fix.

            # If col 0, we take the href as main link.
            if self._td_index == 0:
                if self._current_href:
                    self._current_main_link = self._current_href

            # If col 1, we ignore "More details" or "Signed Copy" links.
            # But we want the title text.
            # Usually the title is direct text in TD, or inside an 'a' if it's a link?
            # In PNBI example: <td ...>Title <a ...>More details</a> ...</td>
            # So the title is text node in TD.
            pass

    def handle_data(self, data: str) -> None:
        if self._in_tr and self._capture_text:
            # Filtering for Col 1 (Title)
            if self._td_index == 1:
                # Specialized logic: ignore "More details", "Signed Copy"
                # If we are inside 'a', check the text?
                if self._in_a:
                    t = data.strip().lower()
                    if "more details" in t or "signed copy" in t:
                        return
                    # Depending on structure, main title might be linked or not.
                    # In PNBI, Title is plain text.

            self._text_parts.append(data)

            if self._td_index == 1 and not self._in_a:
                # Collect title parts outside of anchors (assuming title is text node)
                # But wait, looking at PNBI snippet:
                # <td class="v notices_title">Practice Notes...<a ...>More details</a>...</td>
                # So title is a text node.
                self._current_title_parts.append(data)


class _CircularLettersParser(HTMLParser):
    """
    Parses structures like:
    <div id="year2026">
      ...
      <table>
         <tr><td><a ...>Title</a></td></tr>
      </table>
    </div>
    """

    def __init__(self, base_url: str, min_year: int) -> None:
        super().__init__()
        self._base_url = base_url
        self._min_year = min_year
        self.hits: list[_DocHit] = []

        self._current_year: int | None = None
        self._in_target_year_div = False
        self._year_div_depth = 0

        self._in_table = False
        self._table_depth = 0
        self._in_tr = False

        self._current_href: str | None = None
        self._current_text_parts: list[str] = []
        self._in_a = False

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {k.lower(): v for k, v in attrs if v is not None}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            div_id = attrs_map.get("id", "")
            if div_id.startswith("year"):
                try:
                    y = int(div_id[4:])
                    if y >= self._min_year:
                        self._current_year = y
                        self._in_target_year_div = True
                        self._year_div_depth = 1
                except ValueError:
                    pass
            elif self._in_target_year_div:
                self._year_div_depth += 1
            return

        if not self._in_target_year_div:
            return

        if t == "table":
            self._in_table = True
            self._table_depth += 1
            return

        if t == "tr" and self._in_table:
            self._in_tr = True
            self._current_href = None
            self._current_text_parts = []
            return

        if t == "a" and self._in_tr:
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._in_target_year_div:
            self._year_div_depth -= 1
            if self._year_div_depth == 0:
                self._in_target_year_div = False
                self._current_year = None
            return

        if not self._in_target_year_div:
            return

        if t == "table" and self._in_table:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
            return

        if t == "tr" and self._in_tr:
            self._in_tr = False
            return

        if t == "a" and self._in_a:
            if self._current_href:
                can = _canonicalize_url(urljoin(self._base_url, self._current_href))
                if can and _path_ext(can) in _ALLOWED_DOC_EXTS:
                    txt = _clean_text(" ".join(self._current_text_parts))
                    if txt:
                        self.hits.append(
                            _DocHit(
                                url=can,
                                name=txt,
                                ref_no=None,
                                date_str=None,
                                tab=None,
                                year=self._current_year,
                            )
                        )
            self._in_a = False
            self._current_href = None
            self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._current_text_parts.append(data)


class Crawler:
    name = "practice_notes_and_circular_letters"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        settings = ctx.settings
        cfg = settings.get("crawlers", {}).get(self.name, {})

        # Default URLs if not in settings
        url_pnap = cfg.get(
            "url_pnap",
            "https://www.bd.gov.hk/en/resources/codes-and-references/practice-notes-and-circular-letters/index_pnap.html",
        )
        url_pnrc = cfg.get(
            "url_pnrc",
            "https://www.bd.gov.hk/en/resources/codes-and-references/practice-notes-and-circular-letters/index_pnrc.html",
        )
        url_joint = cfg.get(
            "url_joint",
            "https://www.bd.gov.hk/en/resources/codes-and-references/practice-notes-and-circular-letters/index_joint.html",
        )
        url_pnbi = cfg.get(
            "url_pnbi",
            "https://www.bd.gov.hk/en/resources/codes-and-references/practice-notes-and-circular-letters/index_pnbi.html",
        )
        url_circulars = cfg.get(
            "url_circulars",
            "https://www.bd.gov.hk/en/resources/codes-and-references/practice-notes-and-circular-letters/index_circulars.html",
        )

        years_back = int(cfg.get("years_back", 10))
        current_year = date.today().year
        min_year = current_year - years_back

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))

        http_cfg = settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        max_retries = int(http_cfg.get("max_retries", 3))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))
        user_agent = str(http_cfg.get("user_agent", "")).strip()

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        pnap_tabs = {
            "pane-A": "ADM",  # Administration
            "pane-B": "APP",  # Application of the BO
            "pane-C": "ADV",  # Advisory
        }

        target_pages = [
            ("PNAP", url_pnap, _PracticeNoteParser, {"tab_map": pnap_tabs}),
            ("PNRC", url_pnrc, _PracticeNoteParser, {}),
            ("JPN", url_joint, _PracticeNoteParser, {}),
            ("PNBI", url_pnbi, _PracticeNoteParser, {}),
            (
                "Circulars",
                url_circulars,
                _CircularLettersParser,
                {"min_year": min_year},
            ),
        ]

        for label, url, parser_cls, kwargs in target_pages:
            if not url:
                continue

            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            try:
                if ctx.debug:
                    print(f"[{self.name}] Fetching {label}: {url}")

                resp = _get_with_retries(
                    session,
                    url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )

                if parser_cls is _PracticeNoteParser:
                    parser = _PracticeNoteParser(base_url=url, **kwargs)
                    parser.feed(resp.text)
                else:
                    parser = _CircularLettersParser(base_url=url, **kwargs)
                    parser.feed(resp.text)

                for hit in parser.hits:
                    if hit.url in seen_urls:
                        continue
                    seen_urls.add(hit.url)

                    meta = {
                        "source_page": label,
                        "page_url": url,
                        "file_ext": "pdf",
                    }
                    if hit.ref_no:
                        meta["ref_no"] = hit.ref_no
                    if hit.date_str:
                        meta["issue_date"] = hit.date_str
                    if hit.tab:
                        meta["tab"] = hit.tab
                    if hit.year:
                        meta["year"] = hit.year

                    out.append(
                        UrlRecord(
                            url=hit.url,
                            name=hit.name or "Untitled",
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta=meta,
                        )
                    )

            except Exception as e:
                if ctx.debug:
                    print(f"[{self.name}] Error processing {label}: {e}")
                # We continue to next page even if one fails
                continue

        out.sort(key=lambda r: (r.url or ""))
        return out
