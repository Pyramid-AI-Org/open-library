from __future__ import annotations

import random
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord


_ALLOWED_DOC_EXTS = {".pdf"}


def _clean_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def _sleep_seconds(seconds: float) -> None:
    if seconds <= 0:
        return
    time.sleep(seconds)


def _compute_backoff_seconds(attempt: int, *, base: float, jitter: float) -> float:
    exp = base * (2**attempt)
    exp = min(exp, 30.0)
    if jitter > 0:
        exp += random.uniform(0.0, jitter)
    return exp


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout_seconds)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    resp.raise_for_status()

                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        _sleep_seconds(float(retry_after))
                    except ValueError:
                        pass

                _sleep_seconds(
                    _compute_backoff_seconds(
                        attempt,
                        base=backoff_base_seconds,
                        jitter=backoff_jitter_seconds,
                    )
                )
                continue

            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_err = exc
            if attempt >= max_retries:
                raise

            _sleep_seconds(
                _compute_backoff_seconds(
                    attempt,
                    base=backoff_base_seconds,
                    jitter=backoff_jitter_seconds,
                )
            )

    assert last_err is not None
    raise last_err


def _canonicalize_url(url: str) -> str | None:
    s = (url or "").strip()
    if not s:
        return None

    lower = s.lower()
    if lower.startswith("javascript:"):
        return None
    if lower.startswith("mailto:"):
        return None
    if lower.startswith("tel:"):
        return None

    parsed = urlparse(s)
    if not parsed.scheme or not parsed.netloc:
        return None

    parsed = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
    )

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    parsed = parsed._replace(path=path)

    return urlunparse(parsed)


def _path_ext(url: str) -> str:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


@dataclass(frozen=True)
class _RowLink:
    href: str
    text: str
    row_title: str | None
    tab: str | None
    section: str | None


class _CodesDesignManualsParser(HTMLParser):
    def __init__(self, *, content_element_id: str) -> None:
        super().__init__()
        self.links: list[_RowLink] = []

        self._content_element_id = content_element_id
        self._content_depth = 0

        self._tab_depth = 0
        self._current_tab: str | None = None

        self._table_depth = 0
        self._in_table = False
        self._current_section: str | None = None

        self._capture_caption = False
        self._caption_parts: list[str] = []

        self._in_tr = False
        self._title_cell_depth = 0
        self._row_links: list[tuple[str, str]] = []

        self._in_a = False
        self._current_href: str | None = None
        self._current_text_parts: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_list(attrs_map: dict[str, str]) -> set[str]:
        raw = attrs_map.get("class", "")
        return {c.strip() for c in raw.split() if c.strip()}

    def _enter_content(self, attrs_map: dict[str, str]) -> None:
        if self._content_depth == 0 and attrs_map.get("id") == self._content_element_id:
            self._content_depth = 1
        elif self._content_depth > 0:
            self._content_depth += 1

    def _exit_content(self) -> None:
        if self._content_depth > 0:
            self._content_depth -= 1
            if self._content_depth == 0:
                self._tab_depth = 0
                self._current_tab = None
                self._table_depth = 0
                self._in_table = False
                self._current_section = None
                self._capture_caption = False
                self._caption_parts = []
                self._in_tr = False
                self._title_cell_depth = 0
                self._row_links = []
                self._in_a = False
                self._current_href = None
                self._current_text_parts = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = self._attrs_to_dict(attrs)
        t = tag.lower()

        self._enter_content(attrs_map)
        if self._content_depth <= 0:
            return

        if self._tab_depth > 0:
            self._tab_depth += 1
        if self._table_depth > 0:
            self._table_depth += 1
        if self._title_cell_depth > 0:
            self._title_cell_depth += 1

        if t == "div" and self._tab_depth == 0:
            tab_id = attrs_map.get("id")
            if tab_id == "pane-A":
                self._tab_depth = 1
                self._current_tab = "Codes of Practice and Design Manuals"
            elif tab_id == "pane-B":
                self._tab_depth = 1
                self._current_tab = "Guidelines"

        if t == "table" and not self._in_table:
            classes = self._class_list(attrs_map)
            if "transformable" in classes and "practice" in classes:
                self._in_table = True
                self._table_depth = 1
                self._current_section = attrs_map.get("title") or None
                self._capture_caption = False
                self._caption_parts = []

        if not self._in_table:
            return

        if t == "caption":
            self._capture_caption = True
            self._caption_parts = []
            return

        if t == "tr":
            self._in_tr = True
            self._row_links = []
            return

        if t == "td" and self._in_tr and self._title_cell_depth == 0:
            classes = self._class_list(attrs_map)
            if "notices_title" in classes:
                self._title_cell_depth = 1
            return

        if t == "a" and self._title_cell_depth > 0:
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if self._content_depth <= 0:
            return

        t = tag.lower()

        if self._capture_caption and t == "caption":
            self._capture_caption = False
            if not self._current_section:
                caption_text = _clean_text("".join(self._caption_parts))
                if caption_text:
                    self._current_section = caption_text
            self._caption_parts = []

        if self._in_a and t == "a":
            if self._current_href:
                text = _clean_text("".join(self._current_text_parts))
                self._row_links.append((self._current_href, text))
            self._in_a = False
            self._current_href = None
            self._current_text_parts = []

        if self._title_cell_depth > 0:
            self._title_cell_depth -= 1
            if self._title_cell_depth == 0:
                self._in_a = False
                self._current_href = None
                self._current_text_parts = []

        if self._in_tr and t == "tr":
            pdf_links = [
                (href, text)
                for href, text in self._row_links
                if _path_ext(href) in _ALLOWED_DOC_EXTS
            ]
            row_title = None
            for href, text in pdf_links:
                if text:
                    row_title = text
                    break
            for href, text in pdf_links:
                self.links.append(
                    _RowLink(
                        href=href,
                        text=text,
                        row_title=row_title,
                        tab=self._current_tab,
                        section=self._current_section,
                    )
                )
            self._in_tr = False
            self._row_links = []

        if self._table_depth > 0:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
                self._current_section = None
                self._capture_caption = False
                self._caption_parts = []

        if self._tab_depth > 0:
            self._tab_depth -= 1
            if self._tab_depth == 0:
                self._current_tab = None

        self._exit_content()

    def handle_data(self, data: str) -> None:
        if self._content_depth <= 0:
            return

        if self._capture_caption:
            self._caption_parts.append(data)
        if self._in_a:
            self._current_text_parts.append(data)


class Crawler:
    """Building Department codes/design manuals and guidelines crawler."""

    name = "codes_design_manuals_and_guidelines"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.bd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                "https://www.bd.gov.hk/en/resources/codes-and-references/"
                "codes-and-design-manuals/index.html",
            )
        ).strip()
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.10))
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

        if request_delay_seconds > 0:
            _sleep_seconds(
                request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
            )

        resp = _get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        parser = _CodesDesignManualsParser(content_element_id=content_element_id)
        parser.feed(resp.text or "")

        seen: set[str] = set()
        out: list[UrlRecord] = []

        for link in parser.links:
            full_url = urljoin(page_url, link.href)
            can = _canonicalize_url(full_url)
            if not can:
                continue
            if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                continue
            if can in seen:
                continue
            seen.add(can)

            row_title = _clean_text(link.row_title or "") or None
            link_text = _clean_text(link.text or "")

            name: str | None
            if row_title and link_text:
                if link_text.lower() == row_title.lower():
                    name = row_title
                else:
                    name = f"{row_title} - {link_text}"
            elif row_title:
                name = row_title
            elif link_text:
                name = link_text
            else:
                name = None

            out.append(
                UrlRecord(
                    url=can,
                    name=name,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={
                        "start_url": page_url,
                        "discovered_from": page_url,
                        "file_ext": "pdf",
                        "tab": link.tab,
                        "section": link.section,
                        "row_title": row_title,
                    },
                )
            )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        return out
