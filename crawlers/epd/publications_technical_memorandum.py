from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

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

_DEFAULT_PAGE_URL = (
    "https://www.epd.gov.hk/epd/english/envir_standards/statutory/esg_stat.html"
)


@dataclass(frozen=True)
class _TablePdfLink:
    href: str
    text: str
    stream: str | None


class _TechnicalMemorandumParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[_TablePdfLink] = []

        self._table_depth = 0
        self._in_row = False
        self._col_index = -1

        self._in_link = False
        self._current_href: str | None = None
        self._current_link_text: list[str] = []
        self._current_td_text: list[str] = []

        self._last_stream: str | None = None
        self._row_stream: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table":
            self._table_depth += 1
            return

        if self._table_depth <= 0:
            return

        if tag == "tr":
            self._in_row = True
            self._col_index = -1
            self._row_stream = None
            return

        if not self._in_row:
            return

        if tag == "td":
            self._col_index += 1
            self._current_td_text = []
            return

        if tag == "a" and self._col_index >= 0:
            self._in_link = True
            self._current_href = None
            self._current_link_text = []
            for key, value in attrs:
                if key.lower() == "href":
                    self._current_href = clean_text(str(value or ""))
                    break

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()

        if tag == "table":
            if self._table_depth > 0:
                self._table_depth -= 1
            return

        if self._table_depth <= 0:
            return

        if tag == "a" and self._in_link:
            self._in_link = False
            if self._col_index == 1 and self._current_href:
                text = clean_text("".join(self._current_link_text))
                stream = self._row_stream or self._last_stream
                self.links.append(
                    _TablePdfLink(href=self._current_href, text=text, stream=stream)
                )
            self._current_href = None
            self._current_link_text = []
            return

        if tag == "td" and self._in_row:
            if self._col_index == 0:
                stream = clean_text("".join(self._current_td_text))
                if stream and stream.lower() != "stream":
                    self._row_stream = stream
                    self._last_stream = stream
            self._current_td_text = []
            return

        if tag == "tr":
            self._in_row = False
            self._col_index = -1
            self._row_stream = None

    def handle_data(self, data: str) -> None:
        if self._table_depth <= 0:
            return

        if self._in_link:
            self._current_link_text.append(data)

        if self._in_row and self._col_index >= 0:
            self._current_td_text.append(data)


class Crawler:
    name = "epd.publications_technical_memorandum"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        http_cfg = ctx.get_http_config()

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = clean_text(str(http_cfg.get("user_agent", "")))
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.10))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        try:
            html = _fetch_html(
                session=session,
                url=page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base=backoff_base,
                backoff_jitter=backoff_jitter,
                request_delay=request_delay,
                request_jitter=request_jitter,
            )
        except Exception as exc:
            logger.warning("[%s] Failed to fetch page %s: %s", self.name, page_url, exc)
            return []

        parser = _TechnicalMemorandumParser(base_url=page_url)
        parser.feed(html)

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()
        for row_link in parser.links:
            if len(out) >= max_total_records:
                break

            pdf_url = _resolve_pdf_url(row_link.href, base_url=page_url)
            if not pdf_url or pdf_url in seen_urls:
                continue

            out.append(
                ctx.make_record(
                    url=pdf_url,
                    name=infer_name_from_link(row_link.text, pdf_url),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "discovered_from": page_url,
                        "stream": row_link.stream,
                    },
                )
            )
            seen_urls.add(pdf_url)

        return out


def _fetch_html(
    *,
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
) -> str:
    _sleep_with_jitter(request_delay, request_jitter)
    response = get_with_retries(
        session,
        url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base,
        backoff_jitter_seconds=backoff_jitter,
    )
    response.encoding = "utf-8"
    return response.text or ""


def _sleep_with_jitter(base_delay: float, jitter: float) -> None:
    delay = max(0.0, float(base_delay))
    jitter_value = max(0.0, float(jitter))
    if jitter_value > 0:
        delay += random.uniform(0.0, jitter_value)
    if delay > 0:
        sleep_seconds(delay)


def _resolve_pdf_url(raw_url: str, *, base_url: str) -> str | None:
    absolute = urljoin(base_url, raw_url)
    canonical = canonicalize_url(absolute, encode_spaces=True)
    if not canonical:
        return None

    parsed = urlparse(canonical)
    if parsed.scheme not in {"http", "https"}:
        return None

    if path_ext(canonical) == ".pdf":
        return canonical

    if parsed.path.lower().endswith("/pdf"):
        return canonical

    return None
