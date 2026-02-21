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

_DEFAULT_ROOT_URL = "https://www.emsd.gov.hk/en/gas_safety/publications/index.html"
_DEFAULT_SUBPAGES = [
    "https://www.emsd.gov.hk/en/gas_safety/publications/general/index.html",
    "https://www.emsd.gov.hk/en/gas_safety/publications/codes_of_practice/index.html",
    "https://www.emsd.gov.hk/en/gas_safety/publications/guidance_notes/index.html",
    "https://www.emsd.gov.hk/en/gas_safety/publications/circular_letters/index.html",
]
_DEFAULT_GENERAL_TC_URL = (
    "https://www.emsd.gov.hk/tc/gas_safety/publications/general/index.html"
)

_NON_EN_LANG_LABELS = (
    "indonesian",
    "bahasa",
    "hindi",
    "nepali",
    "punjabi",
    "tagalog",
    "thai",
    "urdu",
    "vietnam",
)


@dataclass(frozen=True)
class _Anchor:
    href: str
    text: str
    lang: str


@dataclass(frozen=True)
class _Cell:
    text: str
    anchors: list[_Anchor]


@dataclass(frozen=True)
class _Row:
    cells: list[_Cell]


class _TableRowsParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self._base_url = base_url
        self.rows: list[_Row] = []

        self._in_tr = False
        self._in_cell = False

        self._current_cells: list[_Cell] = []
        self._current_cell_text_parts: list[str] = []
        self._current_cell_anchors: list[_Anchor] = []

        self._in_a = False
        self._current_href: str | None = None
        self._current_lang: str = ""
        self._current_a_text_parts: list[str] = []

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

        if t == "tr":
            self._in_tr = True
            self._current_cells = []
            return

        if not self._in_tr:
            return

        if t in ("td", "th"):
            self._in_cell = True
            self._current_cell_text_parts = []
            self._current_cell_anchors = []
            return

        if t == "a" and self._in_cell:
            href = attrs_map.get("href")
            if href:
                self._in_a = True
                self._current_href = urljoin(self._base_url, href)
                self._current_lang = attrs_map.get("lang", "")
                self._current_a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "a" and self._in_a:
            self._in_a = False
            if self._current_href:
                self._current_cell_anchors.append(
                    _Anchor(
                        href=self._current_href,
                        text=clean_text("".join(self._current_a_text_parts)),
                        lang=clean_text(self._current_lang),
                    )
                )
            self._current_href = None
            self._current_lang = ""
            self._current_a_text_parts = []
            return

        if t in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._current_cells.append(
                _Cell(
                    text=clean_text("".join(self._current_cell_text_parts)),
                    anchors=self._current_cell_anchors,
                )
            )
            self._current_cell_text_parts = []
            self._current_cell_anchors = []
            return

        if t == "tr" and self._in_tr:
            self._in_tr = False
            if self._current_cells:
                self.rows.append(_Row(cells=self._current_cells))
            self._current_cells = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell_text_parts.append(data)
        if self._in_a:
            self._current_a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_item_no(text: str) -> int | None:
    m = re.search(r"(\d+)", text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _is_probably_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def _anchor_lang_group(a: _Anchor) -> str:
    lang = (a.lang or "").lower()
    text = (a.text or "").lower()
    href = (a.href or "").lower()

    if "zh" in lang or "chinese" in text or _is_probably_chinese(a.text):
        return "zh"

    if any(token in text for token in _NON_EN_LANG_LABELS):
        return "other"

    if "/filemanager/tc/" in href or "/tc/" in href:
        return "zh"

    if "en" in lang:
        return "en"

    # Default to English for EN page links without explicit language marker.
    return "en"


def _extract_rows(html: str, *, page_url: str) -> list[_Row]:
    parser = _TableRowsParser(base_url=page_url)
    parser.feed(html or "")
    return parser.rows


class Crawler:
    name = "emsd.gas_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        root_url = str(cfg.get("root_url", _DEFAULT_ROOT_URL)).strip()
        general_tc_url = str(cfg.get("general_tc_url", _DEFAULT_GENERAL_TC_URL)).strip()

        configured_subpages = cfg.get("subpages", _DEFAULT_SUBPAGES)
        subpages = [str(u).strip() for u in configured_subpages if str(u).strip()]

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        seen_urls: set[str] = set()
        out: list[UrlRecord] = []

        def _emit(pdf_url: str, name: str | None, discovered_from: str, extra_meta: dict | None = None) -> None:
            can = _canonicalize(pdf_url)
            if not can:
                return
            if path_ext(can) != ".pdf":
                return
            if can in seen_urls:
                return

            meta = {"discovered_from": discovered_from}
            if extra_meta:
                meta.update(extra_meta)

            out.append(
                UrlRecord(
                    url=can,
                    name=clean_text(name) or infer_name_from_link(name or "", can),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta=meta,
                )
            )
            seen_urls.add(can)

        def _fetch(url: str) -> str:
            if request_delay > 0:
                sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))
            resp = get_with_retries(
                session,
                url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            resp.encoding = "utf-8"
            return resp.text or ""

        # Fetch root for visibility/debugging (subpages are config-driven for stability).
        try:
            _fetch(root_url)
        except Exception as exc:
            if ctx.debug:
                logger.warning(f"[{self.name}] Failed to fetch root {root_url}: {exc}")

        starred_items: set[int] = set()

        for page_url in subpages:
            try:
                html = _fetch(page_url)
            except Exception as exc:
                logger.error(f"[{self.name}] Failed to fetch {page_url}: {exc}")
                continue

            rows = _extract_rows(html, page_url=page_url)
            is_general = "/publications/general/" in page_url
            is_circular = "/publications/circular_letters/" in page_url

            for row in rows:
                if len(out) >= max_total_records:
                    break

                if not row.cells:
                    continue

                item_no = _extract_item_no(row.cells[0].text if row.cells else "")
                row_is_starred = "*" in (row.cells[0].text if row.cells else "")
                if is_general and row_is_starred and item_no is not None:
                    starred_items.add(item_no)

                anchors_in_row: list[_Anchor] = []
                for cell in row.cells:
                    anchors_in_row.extend(cell.anchors)
                if not anchors_in_row:
                    continue

                date_of_issue = ""
                if is_circular and len(row.cells) > 1:
                    date_of_issue = row.cells[1].text

                for a in anchors_in_row:
                    lang_group = _anchor_lang_group(a)

                    if is_general and lang_group == "other":
                        continue

                    extra_meta: dict = {}
                    if is_circular and date_of_issue:
                        extra_meta["date_of_issue"] = date_of_issue

                    _emit(
                        pdf_url=a.href,
                        name=a.text,
                        discovered_from=page_url,
                        extra_meta=extra_meta,
                    )

            if len(out) >= max_total_records:
                break

        # EN General starred rows are Chinese-only. Pull matching rows from TC page.
        if starred_items and len(out) < max_total_records:
            try:
                tc_html = _fetch(general_tc_url)
                tc_rows = _extract_rows(tc_html, page_url=general_tc_url)

                for row in tc_rows:
                    if len(out) >= max_total_records:
                        break

                    if not row.cells:
                        continue

                    item_no = _extract_item_no(row.cells[0].text if row.cells else "")
                    if item_no is None or item_no not in starred_items:
                        continue

                    anchors_in_row: list[_Anchor] = []
                    for cell in row.cells:
                        anchors_in_row.extend(cell.anchors)

                    for a in anchors_in_row:
                        _emit(
                            pdf_url=a.href,
                            name=a.text,
                            discovered_from=general_tc_url,
                        )
            except Exception as exc:
                logger.error(f"[{self.name}] Failed to fetch TC general page: {exc}")

        out.sort(key=lambda r: (r.url or ""))
        return out
