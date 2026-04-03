from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    get_with_retries,
    normalize_publish_date,
    path_ext,
)


_LAST_UPDATED_RE = re.compile(
    r"last\s+updated\s+in\s+([A-Za-z]+\s+\d{4})",
    re.IGNORECASE,
)


@dataclass
class _ChapterRow:
    chapter: str | None
    full_href: str | None


class _HkpsgTableParser(HTMLParser):
    """Parse HKPSG chapter rows from the main table.

    Expected row shape:
      - td[0]: chapter text
      - td[1]: full version anchor href
      - td[2]: summary version anchor href
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_ChapterRow] = []

        self._in_table = False
        self._table_depth = 0
        self._in_tr = False
        self._in_th = False
        self._td_index = -1

        self._capture_text = False
        self._current_text_parts: list[str] = []

        self._chapter_parts: list[str] = []
        self._full_href: str | None = None

    def _attrs_to_dict(self, attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_table and t == "table":
            self._in_table = True
            self._table_depth = 1
            return

        if not self._in_table:
            return

        self._table_depth += 1

        if t == "tr":
            self._in_tr = True
            self._in_th = False
            self._td_index = -1
            self._chapter_parts = []
            self._full_href = None
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

        if t == "a" and self._td_index == 1 and self._full_href is None:
            href = attrs_map.get("href")
            if href:
                self._full_href = href

    def handle_endtag(self, tag: str) -> None:
        if not self._in_table:
            return

        t = tag.lower()

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
            text = " ".join("".join(self._current_text_parts).split())
            if self._td_index == 0 and text:
                self._chapter_parts.append(text)
            self._capture_text = False
            self._current_text_parts = []
            return

        if t == "tr":
            self._in_tr = False

            chapter = " ".join(self._chapter_parts).strip() or None
            if chapter and self._full_href:
                self.rows.append(_ChapterRow(chapter=chapter, full_href=self._full_href))

            self._td_index = -1
            self._capture_text = False
            self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if not self._in_table or not self._in_tr or self._in_th or not self._capture_text:
            return
        self._current_text_parts.append(data)


class Crawler:
    """PlanD HKPSG crawler for full-version chapter PDFs."""

    name = "standards_guidelines"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        base_url = str(cfg.get("base_url", "https://www.pland.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get("page_url", f"{base_url}/pland_en/tech_doc/hkpsg/index.html")
        ).strip()

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

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

        publish_date: str | None = None
        last_updated_raw: str | None = None
        date_match = _LAST_UPDATED_RE.search(html)
        if date_match:
            last_updated_raw = date_match.group(1).strip()
            publish_date = normalize_publish_date(last_updated_raw)

        parser = _HkpsgTableParser()
        parser.feed(html)

        out: list[UrlRecord] = []
        seen: set[str] = set()

        for row in parser.rows:
            if not row.full_href:
                continue

            abs_url = urljoin(page_url, row.full_href)
            can_url = canonicalize_url(abs_url, encode_spaces=True)
            if not can_url:
                continue

            if "/full/" not in can_url.lower():
                continue
            if path_ext(can_url) != ".pdf":
                continue

            if can_url in seen:
                continue
            seen.add(can_url)

            chapter = (row.chapter or "").strip()
            if not chapter:
                continue

            out.append(
                ctx.make_record(
                    url=can_url,
                    name=f"HKPSG - {chapter}",
                    discovered_at_utc=ctx.started_at_utc,
                    publish_date=publish_date,
                    source=self.name,
                    meta={
                        "chapter": chapter,
                        "discovered_from": page_url
                    },
                )
            )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url, r.name or ""))
        return out
