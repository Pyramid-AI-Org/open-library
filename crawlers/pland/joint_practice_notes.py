from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import re
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


_DEFAULT_PAGE_URL = "https://www.pland.gov.hk/pland_en/tech_doc/jpn/index.html"
_ALLOWED_DOC_EXTS = {".pdf"}
_PDF_FILE_TRAILER_RE = re.compile(r"\(\s*pdf\s+file[^)]*\)\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class _JpnRow:
    jpn_no: str | None
    title: str | None
    href: str | None


class _JointPracticeNotesParser(HTMLParser):
    """Parse PlanD Joint Practice Notes rows from the main table."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_JpnRow] = []

        self._in_table = False
        self._table_depth = 0
        self._in_tr = False
        self._in_td = False
        self._in_th = False
        self._td_index = -1

        self._capture_text = False
        self._text_parts: list[str] = []

        self._current_jpn_no: str | None = None
        self._current_title: str | None = None
        self._current_href: str | None = None

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t == "table":
            if self._table_depth == 0:
                self._in_table = True
            self._table_depth += 1
            return

        if not self._in_table:
            return

        if t == "tr":
            self._in_tr = True
            self._in_td = False
            self._in_th = False
            self._td_index = -1
            self._current_jpn_no = None
            self._current_title = None
            self._current_href = None
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            return

        if t == "td":
            self._in_td = True
            self._td_index += 1
            self._capture_text = True
            self._text_parts = []
            return

        if t == "a" and self._td_index == 1 and self._current_href is None:
            href = self._attrs_to_dict(attrs).get("href")
            if href:
                self._current_href = href

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "table":
            if self._table_depth > 0:
                self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
                self._in_tr = False
                self._in_td = False
                self._in_th = False
                self._capture_text = False
            return

        if not self._in_table or not self._in_tr:
            return

        if t == "th":
            self._in_th = False
            return

        if t == "td":
            text = clean_text("".join(self._text_parts))

            if not self._in_th:
                if self._td_index == 0:
                    self._current_jpn_no = text or None
                elif self._td_index == 1:
                    self._current_title = text or None

            self._in_td = False
            self._capture_text = False
            self._text_parts = []
            return

        if t == "tr":
            if self._current_href and self._current_title:
                self.rows.append(
                    _JpnRow(
                        jpn_no=self._current_jpn_no,
                        title=self._current_title,
                        href=self._current_href,
                    )
                )

            self._in_tr = False
            self._in_td = False
            self._in_th = False
            self._capture_text = False
            self._text_parts = []
            self._td_index = -1

    def handle_data(self, data: str) -> None:
        if not self._in_table or not self._in_tr or not self._capture_text:
            return
        self._text_parts.append(data)


class Crawler:
    """PlanD Joint Practice Notes crawler."""

    name = "joint_practice_notes"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
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
        html = resp.text or ""

        parser = _JointPracticeNotesParser()
        parser.feed(html)

        out: list[UrlRecord] = []
        seen: set[str] = set()

        for row in parser.rows:
            if not row.href:
                continue

            abs_url = urljoin(page_url, row.href)
            can_url = canonicalize_url(abs_url, encode_spaces=True)
            if not can_url:
                continue
            if path_ext(can_url) not in _ALLOWED_DOC_EXTS:
                continue
            if can_url in seen:
                continue

            title = clean_text(row.title or "")
            title = _PDF_FILE_TRAILER_RE.sub("", title).strip()
            display_name = title

            out.append(
                ctx.make_record(
                    url=can_url,
                    name=display_name or None,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={"discovered_from": page_url},
                )
            )
            seen.add(can_url)

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url, r.name or ""))
        return out
