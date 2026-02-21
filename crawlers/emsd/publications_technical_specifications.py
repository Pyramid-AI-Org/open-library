from __future__ import annotations

import logging
import random
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_URL = "https://www.emsd.gov.hk/en/publications/technical_specifications/index.html"

_ISSUE_INFO_RE = re.compile(
    r"\(\s*Issue\s*No\.?\s*[:.]?\s*([^,\)]+)\s*,\s*Issue\s*Date\s*[:.]?\s*([^\)]+)\)",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class _Row:
    ref: str
    desc: str
    pdf_href: str | None
    pdf_text: str


class _SpecTableParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.rows: list[_Row] = []

        self._in_target_table = False
        self._table_depth = 0
        self._in_tr = False
        self._in_td = False
        self._in_th = False

        self._col_idx = -1
        self._row_has_td = False

        self._cell_text_parts: list[str] = []
        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

        self._ref = ""
        self._desc = ""
        self._pdf_href: str | None = None
        self._pdf_text = ""

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_target_table and t == "table":
            classes = self._class_set(attrs_map)
            if "plain_table" in classes:
                self._in_target_table = True
                self._table_depth = 1
                return

        if not self._in_target_table:
            return

        if t == "table":
            self._table_depth += 1
            return

        if t == "tr":
            self._in_tr = True
            self._in_td = False
            self._in_th = False
            self._col_idx = -1
            self._row_has_td = False
            self._ref = ""
            self._desc = ""
            self._pdf_href = None
            self._pdf_text = ""
            return

        if not self._in_tr:
            return

        if t in {"td", "th"}:
            self._in_td = t == "td"
            self._in_th = t == "th"
            if self._in_td:
                self._row_has_td = True
                self._col_idx += 1
            self._cell_text_parts = []
            return

        if t == "a" and self._in_td:
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_target_table:
            return

        if t == "table":
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_target_table = False
            return

        if t == "a" and self._in_a:
            a_text = clean_text("".join(self._a_text_parts))
            if self._in_td and self._col_idx == 2 and self._a_href and not self._pdf_href:
                self._pdf_href = urljoin(self.base_url, self._a_href)
                self._pdf_text = a_text
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            return

        if t in {"td", "th"} and (self._in_td or self._in_th):
            text = clean_text("".join(self._cell_text_parts))
            if self._in_td:
                if self._col_idx == 0:
                    self._ref = text
                elif self._col_idx == 1:
                    self._desc = text
            self._in_td = False
            self._in_th = False
            self._cell_text_parts = []
            return

        if t == "tr" and self._in_tr:
            self._in_tr = False
            if not self._row_has_td:
                return
            self.rows.append(
                _Row(
                    ref=self._ref,
                    desc=self._desc,
                    pdf_href=self._pdf_href,
                    pdf_text=self._pdf_text,
                )
            )

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._a_text_parts.append(data)
        if self._in_td or self._in_th:
            self._cell_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_issue_fields(desc: str) -> tuple[str, str | None, str | None]:
    text = clean_text(desc)
    m = _ISSUE_INFO_RE.search(text)
    if not m:
        return text, "-", None

    issue_no = clean_text(m.group(1)) or "-"
    issue_date = clean_text(m.group(2)) or None

    name = clean_text(text[: m.start()])
    if not name:
        name = text

    return name, issue_no, issue_date


class Crawler:
    name = "emsd.publications_technical_specifications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        try:
            resp = get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
        except Exception as exc:
            logger.error(f"[{self.name}] Failed to fetch {page_url}: {exc}")
            return []

        resp.encoding = "utf-8"

        parser = _SpecTableParser(base_url=page_url)
        parser.feed(resp.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for row in parser.rows:
            can = _canonicalize(row.pdf_href or "")
            if not can:
                continue
            if path_ext(can) != ".pdf":
                continue
            if can in seen_urls:
                continue

            base_name, issue_no, issue_date = _extract_issue_fields(row.desc)
            name = clean_text(base_name)
            if row.ref:
                name = clean_text(f"{row.ref} - {name}") if name else clean_text(row.ref)
            if not name:
                name = clean_text(row.pdf_text) or infer_name_from_link(row.pdf_text, can)

            out.append(
                UrlRecord(
                    url=can,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "issue_no": issue_no or "-",
                        "issue_date": issue_date,
                    },
                )
            )
            seen_urls.add(can)

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        logger.info(f"[{self.name}] Found {len(out)} technical specification PDFs")
        return out
