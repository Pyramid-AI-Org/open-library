from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import random
import re
from urllib.parse import urlparse

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    sleep_seconds,
)
from utils.html_links import extract_links


_YEAR_FROM_TEXT_RE = re.compile(
    r"annual\s+transport\s+digest\s*(?:-|:)?\s*(\d{4})", re.IGNORECASE
)
_YEAR_FROM_PATH_RE = re.compile(r"/mini_site/atd/(\d{4})/")
_SECTION_URL_RE_TEMPLATE = (
    r"^https://www\.td\.gov\.hk/mini_site/atd/{year}/{locale}/"
    r"section(\d+)(?:-(\d+))?\.html$"
)


@dataclass(frozen=True)
class _QueueItem:
    url: str
    discovered_from: str
    link_text: str


@dataclass(frozen=True)
class _FetchedPage:
    url: str
    html: str
    heading: str


class _HeadingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_tag: str | None = None
        self._parts: list[str] = []
        self.h1: str = ""
        self.h2: str = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "h1" and not self.h1:
            self._capture_tag = "h1"
            self._parts = []
        elif t == "h2" and not self.h2:
            self._capture_tag = "h2"
            self._parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if self._capture_tag and t == self._capture_tag:
            text = clean_text("".join(self._parts))
            if self._capture_tag == "h1" and text:
                self.h1 = text
            elif self._capture_tag == "h2" and text:
                self.h2 = text
            self._capture_tag = None
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_tag:
            self._parts.append(data)


def _section_pattern(year: int, locale: str) -> re.Pattern[str]:
    return re.compile(
        _SECTION_URL_RE_TEMPLATE.format(year=year, locale=re.escape(locale)),
        re.IGNORECASE,
    )


def _extract_latest_year(html: str) -> int | None:
    years = [int(v) for v in _YEAR_FROM_TEXT_RE.findall(html or "")]
    if years:
        return max(years)

    years = [int(v) for v in _YEAR_FROM_PATH_RE.findall(html or "")]
    if years:
        return max(years)

    return None


def _extract_heading(html: str) -> str:
    parser = _HeadingParser()
    parser.feed(html or "")
    return clean_text(parser.h1 or parser.h2)


def _parse_section_nums(url: str, pattern: re.Pattern[str]) -> tuple[int, int]:
    m = pattern.match(url)
    if not m:
        return (10_000, 10_000)

    sec = int(m.group(1))
    sub = int(m.group(2)) if m.group(2) else 0
    return (sec, sub)


class Crawler:
    name = "annual_digest"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        annual_index_url = str(
            cfg.get(
                "annual_index_url",
                "https://www.td.gov.hk/en/publications_and_press_releases/publications/free_publications/annual_transport_digest/index.html",
            )
        ).strip()
        base_url = str(cfg.get("base_url", "https://www.td.gov.hk")).rstrip("/")

        locales_cfg = cfg.get("locales", ["en", "tc"])
        locales = [clean_text(str(v)).lower() for v in locales_cfg if clean_text(str(v))]
        locales = [v for v in locales if v in {"en", "tc"}]
        if not locales:
            locales = ["en", "tc"]

        max_section_probe = int(cfg.get("max_section_probe", 24))
        consecutive_probe_miss_stop = int(cfg.get("consecutive_probe_miss_stop", 4))

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
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

        index_resp = get_with_retries(
            session,
            annual_index_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )
        index_resp.encoding = "utf-8"

        latest_year = _extract_latest_year(index_resp.text or "")
        if latest_year is None:
            return []

        publish_date = f"{latest_year:04d}-01-01"

        seen_pages: set[str] = set()
        name_hint_by_url: dict[str, str] = {}
        section_name_by_key: dict[tuple[str, int], str] = {}
        discovered_from_by_url: dict[str, str] = {}
        fetched_pages: dict[str, _FetchedPage] = {}

        for locale in locales:
            section_pattern = _section_pattern(latest_year, locale)
            locale_root = f"{base_url}/mini_site/atd/{latest_year}/{locale}/"
            missing_run = 0

            queue: list[_QueueItem] = []
            for idx in range(1, max_section_probe + 1):
                probe_url = canonicalize_url(
                    f"{locale_root}section{idx}.html", encode_spaces=True
                )
                if not probe_url:
                    continue

                page = self._fetch_page(
                    session=session,
                    url=probe_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                if page is None:
                    missing_run += 1
                    if missing_run >= consecutive_probe_miss_stop:
                        break
                    continue

                missing_run = 0
                queue.append(
                    _QueueItem(
                        url=probe_url,
                        discovered_from=locale_root,
                        link_text=page.heading,
                    )
                )
                fetched_pages[probe_url] = page

            while queue:
                item = queue.pop(0)
                if item.url in seen_pages:
                    continue
                if len(seen_pages) >= max_total_records:
                    break

                page = fetched_pages.get(item.url)
                if page is None:
                    page = self._fetch_page(
                        session=session,
                        url=item.url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_jitter_seconds=backoff_jitter_seconds,
                    )
                if page is None:
                    continue

                fetched_pages[item.url] = page
                seen_pages.add(item.url)

                if item.link_text:
                    name_hint_by_url.setdefault(item.url, clean_text(item.link_text))
                discovered_from_by_url.setdefault(item.url, item.discovered_from)

                sec_num, _ = _parse_section_nums(item.url, section_pattern)
                if sec_num != 10_000 and page.heading:
                    section_name_by_key.setdefault((locale, sec_num), page.heading)

                links = extract_links(page.html, base_url=page.url)
                for link in links:
                    can = canonicalize_url(link.href, encode_spaces=True)
                    if not can:
                        continue

                    m = section_pattern.match(can)
                    if not m:
                        continue

                    if clean_text(link.text):
                        name_hint_by_url.setdefault(can, clean_text(link.text))

                    if can not in seen_pages:
                        queue.append(
                            _QueueItem(
                                url=can,
                                discovered_from=item.url,
                                link_text=link.text,
                            )
                        )

                if request_delay_seconds > 0:
                    sleep_seconds(
                        request_delay_seconds
                        + random.uniform(0.0, max(0.0, request_jitter_seconds))
                    )

        out: list[UrlRecord] = []
        for page_url in sorted(seen_pages):
            parsed = urlparse(page_url)
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) < 5:
                continue

            year_in_url = parts[2]
            locale = parts[3]
            section_pattern = _section_pattern(int(year_in_url), locale)
            sec_num, _ = _parse_section_nums(page_url, section_pattern)

            page = fetched_pages.get(page_url)
            heading = page.heading if page else ""
            section_name = section_name_by_key.get((locale, sec_num)) or heading
            name = name_hint_by_url.get(page_url) or heading or infer_name_from_link(None, page_url)

            out.append(
                ctx.make_record(
                    url=page_url,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    publish_date=publish_date,
                    meta={
                        "section": section_name,
                        "locale": locale,
                        "discovered_from": discovered_from_by_url.get(page_url)
                        or annual_index_url,
                    },
                )
            )

        out.sort(
            key=lambda r: (
                str(r.meta.get("locale") or ""),
                str(r.meta.get("section") or ""),
                r.url,
            )
        )
        return out

    @staticmethod
    def _fetch_page(
        *,
        session: requests.Session,
        url: str,
        timeout_seconds: int,
        max_retries: int,
        backoff_base_seconds: float,
        backoff_jitter_seconds: float,
    ) -> _FetchedPage | None:
        try:
            response = get_with_retries(
                session,
                url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                return None
            raise

        response.encoding = "utf-8"
        html = response.text or ""
        heading = _extract_heading(html)
        return _FetchedPage(url=url, html=html, heading=heading)
