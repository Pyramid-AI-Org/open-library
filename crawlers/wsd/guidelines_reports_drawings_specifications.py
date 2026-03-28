from __future__ import annotations

import random
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
    normalize_publish_date,
    path_ext,
    sleep_seconds,
)
from utils.html_links import HtmlLink, extract_links, extract_links_in_element


_DEFAULT_PAGE_URL = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/"
)
_DEFAULT_EXCAVATION_SUBPAGE = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/guidelines-excavation/index.html"
)
_DEFAULT_PREVENT_DAMAGE_SUBPAGE = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/prevent-damage/index.html"
)
_DEFAULT_MAINLAYING_SUBPAGE = (
    "https://www.wsd.gov.hk/en/publications-and-statistics/"
    "guidelines-reports-drawings-specifications/mainlaying-practice/index.html"
)


@dataclass(frozen=True)
class _TableCell:
    text: str
    classes: set[str]
    links: list[tuple[str, str]]


@dataclass(frozen=True)
class _TableRow:
    classes: set[str]
    cells: list[_TableCell]


class _PageTitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_depth = 0
        self._capture_key: str | None = None
        self._text_parts: list[str] = []

        self.page_title: str | None = None
        self.first_heading: str | None = None

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if self._capture_depth > 0:
            self._capture_depth += 1
            return

        if t == "h2" and attrs_map.get("id", "").strip().lower() == "page_title":
            self._capture_depth = 1
            self._capture_key = "page_title"
            self._text_parts = []
            return

        if t in {"h1", "h2"} and self.first_heading is None:
            self._capture_depth = 1
            self._capture_key = "first_heading"
            self._text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if self._capture_depth <= 0:
            return

        self._capture_depth -= 1
        if self._capture_depth > 0:
            return

        text = clean_text("".join(self._text_parts))
        if text:
            if self._capture_key == "page_title":
                self.page_title = text
            elif self._capture_key == "first_heading" and self.first_heading is None:
                self.first_heading = text

        self._capture_key = None
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_depth > 0:
            self._text_parts.append(data)


class _SimpleTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_TableRow] = []

        self._row_stack: list[list[_TableCell]] = []
        self._row_class_stack: list[set[str]] = []

        self._in_cell = False
        self._cell_text_parts: list[str] = []
        self._cell_links: list[tuple[str, str]] = []
        self._cell_classes: set[str] = set()

        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

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
        return {
            c.strip().lower() for c in attrs_map.get("class", "").split() if c.strip()
        }

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t == "tr":
            attrs_map = self._attrs_to_dict(attrs)
            self._row_stack.append([])
            self._row_class_stack.append(self._class_set(attrs_map))
            return

        if not self._row_stack:
            return

        if t in {"td", "th"} and not self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            self._in_cell = True
            self._cell_text_parts = []
            self._cell_links = []
            self._cell_classes = self._class_set(attrs_map)
            return

        if t == "a" and self._in_cell:
            attrs_map = self._attrs_to_dict(attrs)
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "a" and self._in_a:
            text = clean_text("".join(self._a_text_parts))
            if self._a_href:
                self._cell_links.append((self._a_href, text))
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            return

        if t in {"td", "th"} and self._in_cell:
            if self._row_stack:
                self._row_stack[-1].append(
                    _TableCell(
                        text=clean_text("".join(self._cell_text_parts)),
                        classes=self._cell_classes,
                        links=self._cell_links,
                    )
                )
            self._in_cell = False
            self._cell_text_parts = []
            self._cell_links = []
            self._cell_classes = set()
            return

        if t == "tr" and self._row_stack:
            row_cells = self._row_stack.pop()
            row_classes = (
                self._row_class_stack.pop() if self._row_class_stack else set()
            )
            if row_cells:
                self.rows.append(_TableRow(classes=row_classes, cells=row_cells))

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_links_scoped(
    html: str, *, base_url: str, content_element_id: str
) -> list[HtmlLink]:
    links = extract_links_in_element(
        html, base_url=base_url, element_id=content_element_id
    )
    if not links:
        links = extract_links(html, base_url=base_url)
    return links


def _extract_page_title(html: str) -> str | None:
    parser = _PageTitleParser()
    parser.feed(html)
    return parser.page_title or parser.first_heading


def _is_updated_version_heading(text: str) -> bool:
    normalized = clean_text(text).lower()
    return "updated version" in normalized and "amendments" in normalized


def _pick_single_pdf_link(links: list[HtmlLink]) -> str | None:
    pdf_urls: list[tuple[str, str]] = []
    for link in links:
        can = _canonicalize(link.href)
        if not can or path_ext(can) != ".pdf":
            continue
        pdf_urls.append((can, clean_text(link.text)))

    if not pdf_urls:
        return None

    for can, text in pdf_urls:
        if "download" in text.lower():
            return can

    return pdf_urls[0][0]


def _extract_mainlaying_latest(
    html: str,
    *,
    subpage_url: str,
    fallback_name: str | None,
) -> tuple[str, str | None, str | None] | None:
    parser = _SimpleTableParser()
    parser.feed(html)

    in_updated_section = False
    for row in parser.rows:
        if not row.cells:
            continue

        row_text = clean_text(" ".join(cell.text for cell in row.cells))
        first_cell_classes = row.cells[0].classes

        if "sub_hd" in first_cell_classes or _is_updated_version_heading(row_text):
            if _is_updated_version_heading(row_text):
                in_updated_section = True
            elif in_updated_section:
                break
            else:
                in_updated_section = False
            continue

        if not in_updated_section:
            continue

        if len(row.cells) < 3:
            continue

        name = clean_text(row.cells[1].text) or fallback_name
        publish_date = normalize_publish_date(row.cells[2].text)

        for cell in row.cells:
            for href, _text in cell.links:
                can = _canonicalize(urljoin(subpage_url, href))
                if not can or path_ext(can) != ".pdf":
                    continue
                return (can, name, publish_date)

    return None


def _as_url_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        return [raw]
    if not isinstance(raw, list):
        return []

    out: list[str] = []
    for value in raw:
        if not isinstance(value, str):
            continue
        text = value.strip()
        if text:
            out.append(text)
    return out


class Crawler:
    name = "guidelines_reports_drawings_specifications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        scope_prefix = (
            str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip().rstrip("/")
            + "/"
        )
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )
        max_total_records = int(cfg.get("max_total_records", 50000))

        target_subpages_raw = _as_url_list(
            cfg.get(
                "target_subpages",
                [
                    _DEFAULT_EXCAVATION_SUBPAGE,
                    _DEFAULT_PREVENT_DAMAGE_SUBPAGE,
                    _DEFAULT_MAINLAYING_SUBPAGE,
                ],
            )
        )
        single_pdf_subpages_raw = _as_url_list(
            cfg.get(
                "single_pdf_subpages",
                [_DEFAULT_EXCAVATION_SUBPAGE, _DEFAULT_PREVENT_DAMAGE_SUBPAGE],
            )
        )
        mainlaying_subpage_raw = str(
            cfg.get("mainlaying_subpage", _DEFAULT_MAINLAYING_SUBPAGE)
        ).strip()
        exclude_subpages_raw = _as_url_list(cfg.get("exclude_subpages", []))

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        def _fetch(url: str) -> str:
            if request_delay > 0:
                sleep_seconds(
                    request_delay + random.uniform(0.0, max(0.0, request_jitter))
                )
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

        target_subpages: list[str] = []
        seen_target_subpages: set[str] = set()
        for raw_url in target_subpages_raw:
            can = _canonicalize(raw_url)
            if not can or can in seen_target_subpages:
                continue
            if not can.startswith(scope_prefix):
                continue
            target_subpages.append(can)
            seen_target_subpages.add(can)

        excluded_subpages: set[str] = set()
        for raw_url in exclude_subpages_raw:
            can = _canonicalize(raw_url)
            if can:
                excluded_subpages.add(can)

        single_pdf_subpages: list[str] = []
        seen_single_pdf_subpages: set[str] = set()
        for raw_url in single_pdf_subpages_raw:
            can = _canonicalize(raw_url)
            if not can or can in seen_single_pdf_subpages:
                continue
            if can not in seen_target_subpages:
                continue
            if can in excluded_subpages:
                continue
            single_pdf_subpages.append(can)
            seen_single_pdf_subpages.add(can)

        mainlaying_subpage = (
            _canonicalize(mainlaying_subpage_raw) or _DEFAULT_MAINLAYING_SUBPAGE
        )
        if (
            mainlaying_subpage in excluded_subpages
            or mainlaying_subpage not in seen_target_subpages
        ):
            mainlaying_subpage = ""

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        landing_html = _fetch(page_url)
        for link in _extract_links_scoped(
            landing_html,
            base_url=page_url,
            content_element_id=content_element_id,
        ):
            can = _canonicalize(link.href)
            if not can or path_ext(can) != ".pdf":
                continue
            if can in seen_pdf_urls:
                continue

            out.append(
                ctx.make_record(
                    url=can,
                    name=clean_text(link.text) or infer_name_from_link(link.text, can),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": page_url},
                )
            )
            seen_pdf_urls.add(can)

            if len(out) >= max_total_records:
                break

        for subpage_url in single_pdf_subpages:
            if len(out) >= max_total_records:
                break

            html = _fetch(subpage_url)
            title = _extract_page_title(html)
            pdf_url = _pick_single_pdf_link(
                _extract_links_scoped(
                    html, base_url=subpage_url, content_element_id=content_element_id
                )
            )
            if not pdf_url or pdf_url in seen_pdf_urls:
                continue

            out.append(
                ctx.make_record(
                    url=pdf_url,
                    name=title or infer_name_from_link(title or "", pdf_url),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": subpage_url},
                )
            )
            seen_pdf_urls.add(pdf_url)

        if mainlaying_subpage and len(out) < max_total_records:
            html = _fetch(mainlaying_subpage)
            title = _extract_page_title(html)
            latest = _extract_mainlaying_latest(
                html, subpage_url=mainlaying_subpage, fallback_name=title
            )
            if latest is not None:
                pdf_url, name, publish_date = latest
                if pdf_url not in seen_pdf_urls:
                    out.append(
                        ctx.make_record(
                            url=pdf_url,
                            name=name or infer_name_from_link(name or "", pdf_url),
                            discovered_at_utc=ctx.run_date_utc,
                            source=self.name,
                            meta={"discovered_from": mainlaying_subpage},
                            publish_date=publish_date,
                        )
                    )
                    seen_pdf_urls.add(pdf_url)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out[:max_total_records]
