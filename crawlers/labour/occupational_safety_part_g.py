from __future__ import annotations

import random
import re
from datetime import datetime
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
    sleep_seconds,
)


_DEFAULT_PAGE_URL_EN = "https://www.labour.gov.hk/eng/public/content2_8g.htm"
_DEFAULT_PAGE_URL_TC = "https://www.labour.gov.hk/tc/public/content2_8g.htm"
_DEFAULT_PAGE_URL_TC_FALLBACK = "https://www.labour.gov.hk/tc_chi/public/content2_8g.htm"


class _SectionGParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self._in_main_content = False
        self._main_content_depth = 0

        self._li_depth = 0
        self._in_a = False
        self._a_href: str | None = None
        self._a_text_parts: list[str] = []

        self.entries: list[tuple[str, str]] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            if self._in_main_content:
                self._main_content_depth += 1
            elif attrs_map.get("id", "").strip().lower() == "maincontent":
                self._in_main_content = True
                self._main_content_depth = 1

        if not self._in_main_content:
            return

        if t == "li":
            self._li_depth += 1
            return

        if t == "a" and self._li_depth > 0:
            self._in_a = True
            self._a_href = attrs_map.get("href")
            self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._in_main_content:
            self._main_content_depth -= 1
            if self._main_content_depth <= 0:
                self._in_main_content = False
                self._main_content_depth = 0
                self._li_depth = 0
                self._in_a = False
                self._a_href = None
                self._a_text_parts = []
            return

        if not self._in_main_content:
            return

        if t == "li" and self._li_depth > 0:
            self._li_depth -= 1
            return

        if t == "a" and self._in_a:
            href = clean_text(self._a_href)
            name = clean_text("".join(self._a_text_parts))
            if href:
                self.entries.append((href, name))
            self._in_a = False
            self._a_href = None
            self._a_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _date_from_name(name: str) -> str | None:
    text = clean_text(name)
    if not text:
        return None

    m = re.search(
        r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return normalize_publish_date(text)

    try:
        day = int(m.group(1))
        month_name = m.group(2).title()
        year = int(m.group(3))
        dt = datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y").date()
        return dt.isoformat()
    except ValueError:
        return normalize_publish_date(text)


def _date_from_url(url: str) -> str | None:
    m = re.search(r"(?:^|[_/\-])(\d{8})(?:[_\-.]|$)", url)
    if not m:
        return None

    token = m.group(1)
    try:
        dt = datetime.strptime(token, "%Y%m%d").date()
        return dt.isoformat()
    except ValueError:
        return None


def _extract_publish_date(name: str, url: str) -> str | None:
    return _date_from_name(name) or _date_from_url(url)


def _fetch_html(
    *,
    session: requests.Session,
    page_url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
) -> str:
    resp = get_with_retries(
        session,
        page_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base,
        backoff_jitter_seconds=backoff_jitter,
    )
    resp.encoding = "utf-8"
    return resp.text or ""


class Crawler:
    name = "occupational_safety_part_g"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url_en = str(cfg.get("page_url_en", _DEFAULT_PAGE_URL_EN)).strip()
        page_url_tc = str(cfg.get("page_url_tc", _DEFAULT_PAGE_URL_TC)).strip()
        page_url_tc_fallback = str(
            cfg.get("page_url_tc_fallback", _DEFAULT_PAGE_URL_TC_FALLBACK)
        ).strip()

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 60))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        listings: list[tuple[str, str]] = [("en", page_url_en)]

        tc_url_used = page_url_tc
        try:
            if request_delay > 0:
                sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))
            _fetch_html(
                session=session,
                page_url=page_url_tc,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base=backoff_base,
                backoff_jitter=backoff_jitter,
            )
        except requests.RequestException:
            tc_url_used = page_url_tc_fallback

        listings.append(("tc", tc_url_used))

        out: list[UrlRecord] = []
        seen_keys: set[tuple[str, str]] = set()

        for locale, listing_url in listings:
            if len(out) >= max_total_records:
                break

            if request_delay > 0:
                sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

            try:
                html = _fetch_html(
                    session=session,
                    page_url=listing_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_jitter=backoff_jitter,
                )
            except requests.RequestException:
                continue

            parser = _SectionGParser()
            parser.feed(html)

            for href, link_name in parser.entries:
                if len(out) >= max_total_records:
                    break

                canonical_url = _canonicalize(urljoin(listing_url, href))
                if not canonical_url:
                    continue

                key = (canonical_url, locale)
                if key in seen_keys:
                    continue

                name = clean_text(link_name) or infer_name_from_link(link_name, canonical_url)
                publish_date = _extract_publish_date(name or "", canonical_url)

                out.append(
                    ctx.make_record(
                        url=canonical_url,
                        name=name,
                        discovered_at_utc=ctx.run_date_utc,
                        publish_date=publish_date,
                        source=self.name,
                        meta={
                            "discovered_from": listing_url,
                            "locale": locale,
                        },
                    )
                )
                seen_keys.add(key)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.meta.get("locale") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
