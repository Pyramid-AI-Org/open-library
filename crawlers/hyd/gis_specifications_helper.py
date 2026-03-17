from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import canonicalize_url, clean_text, get_with_retries, path_ext


GIS_PAGE_URL = (
    "https://www.hyd.gov.hk/en/technical_references/technical_document/"
    "GIS_Specifications/index.html"
)

_ALLOWED_DOC_EXTS = {".pdf"}
_TRACK_CHANGE_RE = re.compile(r"track[\s._-]*change(s)?", re.IGNORECASE)
_RELEASED_ON_RE = re.compile(
    r"released\s+on\s+(\d{1,2})\.(\d{1,2})\.(\d{4})", re.IGNORECASE
)
_VERSION_RE = re.compile(r"\b(version\s+\d+(?:\.\d+)*)\b", re.IGNORECASE)


@dataclass(frozen=True)
class GisSpecificationHit:
    url: str
    name: str
    publish_date: str | None


@dataclass
class _Row:
    base_title: str | None
    links: list[tuple[str, str]]


class _GisTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_tr = False
        self._tr_depth = 0
        self._current_row: _Row | None = None

        self._in_p = False
        self._p_parts: list[str] = []

        self._in_a = False
        self._a_href: str | None = None
        self._a_parts: list[str] = []

        self.rows: list[_Row] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "tr":
            if not self._in_tr:
                self._in_tr = True
                self._tr_depth = 1
                self._current_row = _Row(base_title=None, links=[])
            else:
                self._tr_depth += 1
            return

        if not self._in_tr:
            return

        if t == "p":
            self._in_p = True
            self._p_parts = []
            return

        if t == "a":
            href = None
            for k, v in attrs:
                if k and k.lower() == "href" and v:
                    href = v
                    break
            self._in_a = True
            self._a_href = href
            self._a_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "p" and self._in_tr and self._in_p and self._current_row:
            text = clean_text("".join(self._p_parts))
            if text and not self._current_row.base_title:
                self._current_row.base_title = text
            self._in_p = False
            self._p_parts = []
            return

        if t == "a" and self._in_tr and self._in_a and self._current_row:
            if self._a_href:
                text = clean_text("".join(self._a_parts))
                self._current_row.links.append((self._a_href, text))
            self._in_a = False
            self._a_href = None
            self._a_parts = []
            return

        if t == "tr" and self._in_tr:
            self._tr_depth -= 1
            if self._tr_depth <= 0:
                self._in_tr = False
                self._tr_depth = 0
                if self._current_row and self._current_row.base_title and self._current_row.links:
                    self.rows.append(self._current_row)
                self._current_row = None
                self._in_p = False
                self._p_parts = []
                self._in_a = False
                self._a_href = None
                self._a_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_p:
            self._p_parts.append(data)
        if self._in_a:
            self._a_parts.append(data)


def _canonicalize_url(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _is_track_change(*, href: str, text: str) -> bool:
    hay = f"{href} {text}".lower()
    return _TRACK_CHANGE_RE.search(hay) is not None


def _extract_publish_date(text: str) -> str | None:
    m = _RELEASED_ON_RE.search(text)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    return f"{int(dd):02d}.{int(mm):02d}.{yyyy}"


def _extract_version(text: str) -> str | None:
    m = _VERSION_RE.search(text)
    if not m:
        return None
    v = clean_text(m.group(1))
    if not v:
        return None
    return v[0].upper() + v[1:]


def fetch_gis_specifications_html(
    *,
    session: requests.Session,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> str:
    resp = get_with_retries(
        session,
        GIS_PAGE_URL,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )
    return resp.text


def parse_gis_specification_hits(
    html: str,
    *,
    page_url: str = GIS_PAGE_URL,
) -> list[GisSpecificationHit]:
    parser = _GisTableParser()
    parser.feed(html)

    out: list[GisSpecificationHit] = []
    seen_urls: set[str] = set()
    for row in parser.rows:
        base_title = clean_text(row.base_title)
        if not base_title:
            continue

        for href, text in row.links:
            absolute = _canonicalize_url(urljoin(page_url, href))
            if not absolute:
                continue
            if path_ext(absolute) not in _ALLOWED_DOC_EXTS:
                continue
            if _is_track_change(href=href, text=text):
                continue
            if absolute in seen_urls:
                continue

            seen_urls.add(absolute)
            version = _extract_version(text)
            name = base_title if not version else f"{base_title} {version}"
            out.append(
                GisSpecificationHit(
                    url=absolute,
                    name=name,
                    publish_date=_extract_publish_date(text),
                )
            )

    return out


def fetch_and_parse_gis_specification_hits(
    *,
    session: requests.Session,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> list[GisSpecificationHit]:
    html = fetch_gis_specifications_html(
        session=session,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )
    return parse_gis_specification_hits(html)
