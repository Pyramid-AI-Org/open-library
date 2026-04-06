from __future__ import annotations

import random
import re
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlparse

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


_DEFAULT_PAGE_URL = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "automatic-meter-reading/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/automatic-meter-reading"
)

_HEADING_WIRED = "wired"
_HEADING_WIRELESS = "wireless"

_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_DAY_MONTH_YEAR_RE = re.compile(
    r"\b([0-3]?\d)\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)\s+(19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
_MONTH_YEAR_TEXT_RE = re.compile(
    r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)\s+(19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
_VERSION_MM_YYYY_RE = re.compile(
    r"\b(?:version\s*)?(0?[1-9]|1[0-2])\s*/\s*(19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
_MONTH_COMPACT_RE = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)"
    r"[-_\s]?((?:19|20)?\d{2})\b",
    re.IGNORECASE,
)


class _AmiSectionPdfParser(HTMLParser):
    def __init__(self, *, content_element_id: str) -> None:
        super().__init__()
        self.content_element_id = content_element_id.strip().lower() or "content"

        self._in_content = False
        self._content_depth = 0

        self._in_heading = False
        self._heading_tag: str | None = None
        self._heading_text_parts: list[str] = []

        self._pending_section: str | None = None
        self._active_section: str | None = None
        self._in_target_list = False
        self._target_list_depth = 0

        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []
        self._a_is_pdf_class = False

        self.links: list[tuple[str, str, str, bool]] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _section_from_heading(text: str) -> str | None:
        normalized = clean_text(text).lower()
        if "wireless smart water metering" in normalized:
            return _HEADING_WIRELESS
        if "wired smart water metering" in normalized:
            return _HEADING_WIRED
        return None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_content:
            if attrs_map.get("id", "").strip().lower() == self.content_element_id:
                self._in_content = True
                self._content_depth = 1
            return

        self._content_depth += 1

        if t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._heading_tag = t
            self._heading_text_parts = []
            self._pending_section = None
            return

        if t == "ul":
            if self._in_target_list:
                self._target_list_depth += 1
                return
            if self._pending_section:
                self._active_section = self._pending_section
                self._pending_section = None
                self._in_target_list = True
                self._target_list_depth = 1
                return

        if t == "a" and self._in_target_list and self._active_section:
            href = attrs_map.get("href")
            if href:
                self._in_a = True
                self._a_href = href
                self._a_text_parts = []
                classes = {
                    c.strip().lower()
                    for c in attrs_map.get("class", "").split()
                    if c.strip()
                }
                self._a_is_pdf_class = "pdf" in classes

    def handle_endtag(self, tag: str) -> None:
        if not self._in_content:
            return

        t = tag.lower()

        if self._in_heading and t == self._heading_tag:
            heading_text = clean_text("".join(self._heading_text_parts))
            self._pending_section = self._section_from_heading(heading_text)
            self._in_heading = False
            self._heading_tag = None
            self._heading_text_parts = []

        if t == "a" and self._in_a:
            if self._a_href and self._active_section:
                self.links.append(
                    (
                        self._a_href,
                        clean_text("".join(self._a_text_parts)),
                        self._active_section,
                        self._a_is_pdf_class,
                    )
                )
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []
            self._a_is_pdf_class = False

        if t == "ul" and self._in_target_list:
            self._target_list_depth -= 1
            if self._target_list_depth <= 0:
                self._in_target_list = False
                self._target_list_depth = 0
                self._active_section = None

        self._content_depth -= 1
        if self._content_depth <= 0:
            self._in_content = False
            self._content_depth = 0

    def handle_data(self, data: str) -> None:
        if self._in_heading:
            self._heading_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _publish_date_from_text(value: str) -> str | None:
    text = clean_text(value.replace("_", " ").replace("-", " "))
    if not text:
        return None

    full = _DAY_MONTH_YEAR_RE.search(text)
    if full:
        day = int(full.group(1))
        month_token = full.group(2).lower()
        month = _MONTHS.get(month_token)
        year = int(full.group(3))
        if month and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"

    version_mm_yyyy = _VERSION_MM_YYYY_RE.search(text)
    if version_mm_yyyy:
        month = int(version_mm_yyyy.group(1))
        year = int(version_mm_yyyy.group(2))
        return f"{year:04d}-{month:02d}-01"

    month_year = _MONTH_YEAR_TEXT_RE.search(text)
    if month_year:
        month_token = month_year.group(1).lower()
        month = _MONTHS.get(month_token)
        year = int(month_year.group(2))
        if month:
            return f"{year:04d}-{month:02d}-01"

    compact = _MONTH_COMPACT_RE.search(text)
    if compact:
        month_token = compact.group(1).lower()
        month = _MONTHS.get(month_token)
        year_text = compact.group(2)
        if month:
            year = int(year_text)
            if len(year_text) == 2:
                year += 2000
            return f"{year:04d}-{month:02d}-01"

    return None


def _extract_publish_date(*, url: str, name: str | None) -> str | None:
    parsed = urlparse(url)
    tail = (parsed.path or "").rstrip("/").rsplit("/", 1)[-1]
    filename = unquote(tail)
    if "." in filename:
        filename = filename.rsplit(".", 1)[0]

    from_filename = _publish_date_from_text(filename)
    from_name = _publish_date_from_text(name or "")

    # Prefer whichever source has explicit day precision (e.g. 14 Nov 2022)
    # to avoid downgrading to YYYY-MM-01 when link text is more specific.
    filename_has_day = bool(_DAY_MONTH_YEAR_RE.search(clean_text(filename)))
    name_has_day = bool(_DAY_MONTH_YEAR_RE.search(clean_text(name or "")))

    if from_filename and from_name:
        if name_has_day and not filename_has_day:
            return from_name
        return from_filename

    if from_filename:
        return from_filename

    return from_name


class Crawler:
    name = "automatic_meter_reading"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        scope_prefix = (
            str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip().rstrip("/")
        )
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )
        max_total_records = int(cfg.get("max_total_records", 50000))

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        seed_url = _canonicalize(page_url)
        if not seed_url:
            return []
        if not (seed_url == scope_prefix or seed_url.startswith(scope_prefix + "/")):
            return []

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        response = get_with_retries(
            session,
            seed_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"
        html = response.text or ""

        parser = _AmiSectionPdfParser(content_element_id=content_element_id)
        parser.feed(html)

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for href, link_text, section, is_pdf_class in parser.links:
            if len(out) >= max_total_records:
                break

            can = _canonicalize(urljoin(seed_url, href))
            if not can:
                continue
            if path_ext(can) != ".pdf" and not is_pdf_class:
                continue

            if can in seen_urls:
                continue

            name = clean_text(link_text) or infer_name_from_link(link_text, can)

            out.append(
                ctx.make_record(
                    url=can,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "discovered_from": seed_url,
                        "section_name": section,
                    },
                    publish_date=_extract_publish_date(url=can, name=name),
                )
            )
            seen_urls.add(can)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.meta.get("section_name") or ""),
            )
        )
        return out