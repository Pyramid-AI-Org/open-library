from __future__ import annotations

import random
import re
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import RunContext, UrlRecord, get_with_retries, sleep_seconds


_PDF_PATH_RE = re.compile(r"^/filemanager/en/.*\.pdf$", re.IGNORECASE)


_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries


def _parse_date_to_iso(value: str) -> str | None:
    # Observed examples:
    # - "18 July 2025"
    # - "28 Jun 2007"
    # - "19 Jul 2006"
    s = " ".join((value or "").strip().split())
    if not s:
        return None

    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


@dataclass
class _Row:
    circular_no: str | None
    title: str | None
    date_iso: str | None
    href: str | None
    section: str | None


class _TechnicalCircularsParser(HTMLParser):
    """Parse Planning & Lands Technical Circulars page.

    The page can contain multiple tables with the same 3-column layout:
    Circular No. | Title | Date
    Each section is preceded by a <strong class="heading2"> heading.
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_Row] = []

        self._current_section: str | None = None

        self._in_heading2 = False
        self._heading_parts: list[str] = []

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_th = False
        self._td_index = -1

        self._capture_text = False
        self._current_text_parts: list[str] = []

        self._current_href: str | None = None
        self._current_circular_no_parts: list[str] = []
        self._current_title_parts: list[str] = []
        self._current_date_parts: list[str] = []

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

        if t == "strong":
            cls = attrs_map.get("class", "")
            if "heading2" in cls.split():
                self._in_heading2 = True
                self._heading_parts = []
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
            self._td_index = -1
            self._current_href = None
            self._current_circular_no_parts = []
            self._current_title_parts = []
            self._current_date_parts = []
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            return

        if t == "td":
            self._td_index += 1
            self._capture_text = True
            self._current_text_parts = []
            return

        if t == "a" and self._td_index == 0 and self._current_href is None:
            href = attrs_map.get("href")
            if href:
                self._current_href = href

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "strong" and self._in_heading2:
            self._in_heading2 = False
            text = " ".join("".join(self._heading_parts).split()).strip()
            self._current_section = text or None
            self._heading_parts = []
            return

        if not self._in_table:
            return

        self._table_depth -= 1
        if self._table_depth == 0:
            self._in_table = False
            self._in_tr = False
            self._in_th = False
            self._capture_text = False
            return

        if t == "th":
            self._in_th = False
            return

        if not self._in_tr:
            return

        if t == "td" and not self._in_th:
            text = " ".join("".join(self._current_text_parts).split()).strip()
            if self._td_index == 0:
                if text:
                    self._current_circular_no_parts.append(text)
            elif self._td_index == 1:
                if text:
                    self._current_title_parts.append(text)
            elif self._td_index == 2:
                if text:
                    self._current_date_parts.append(text)

            self._capture_text = False
            self._current_text_parts = []
            return

        if t == "tr":
            self._in_tr = False

            # Skip header rows (no href).
            if self._current_href:
                circular_no = " ".join(self._current_circular_no_parts).strip() or None
                title = " ".join(self._current_title_parts).strip() or None
                date_raw = " ".join(self._current_date_parts).strip()
                date_iso = _parse_date_to_iso(date_raw)

                self.rows.append(
                    _Row(
                        circular_no=circular_no,
                        title=title,
                        date_iso=date_iso,
                        href=self._current_href,
                        section=self._current_section,
                    )
                )

            self._td_index = -1
            self._capture_text = False
            self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_heading2:
            self._heading_parts.append(data)
            return

        if not self._in_table or not self._in_tr or self._in_th:
            return
        if not self._capture_text:
            return
        self._current_text_parts.append(data)


class Crawler:
    """Crawl DevB Planning and Lands Technical Circulars (English).

    Page:
      https://www.devb.gov.hk/en/publications_and_press_releases/technical_circulars/planning_and_lands_technical_circular/index.html

    Extracts PDF URLs plus title and date.

    Config: crawlers.devb_planning_and_lands_technical_circulars
      - base_url: https://www.devb.gov.hk
      - page_url: (optional override)
      - max_total_records: 50000
      - backoff_base_seconds: 0.5
      - backoff_jitter_seconds: 0.25
    """

    name = "devb_planning_and_lands_technical_circulars"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.devb.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                f"{base_url}/en/publications_and_press_releases/technical_circulars/planning_and_lands_technical_circular/index.html",
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

        resp = _get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        parser = _TechnicalCircularsParser()
        parser.feed(resp.text)

        seen: set[str] = set()
        out: list[UrlRecord] = []

        for row in parser.rows:
            if not row.href:
                continue

            abs_url = urljoin(page_url, row.href)
            if not abs_url.startswith(base_url + "/"):
                continue

            # This page is English; keep only English filemanager PDFs.
            path = abs_url[len(base_url) :]
            if not _PDF_PATH_RE.match(path):
                continue

            if abs_url in seen:
                continue
            seen.add(abs_url)

            name_parts: list[str] = []
            if row.circular_no:
                name_parts.append(row.circular_no)
            if row.title:
                name_parts.append(row.title)
            name = " - ".join(name_parts) or None

            out.append(
                UrlRecord(
                    url=abs_url,
                    name=name,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={
                        "circular_no": row.circular_no,
                        "title": row.title,
                        "date": row.date_iso,
                        "section": row.section,
                        "listing_url": page_url,
                    },
                )
            )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url, (r.meta.get("date") or "")))
        return out
