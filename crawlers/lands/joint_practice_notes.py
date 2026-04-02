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
    path_ext,
    sleep_seconds,
)

_DEFAULT_PAGE_URL = "https://www.landsd.gov.hk/en/resources/practice-notes/jpn.html"


@dataclass(frozen=True)
class _RowData:
    subject: str
    main_href: str | None
    accessible_href: str | None


class _JpnDesktopTableParser(HTMLParser):
    """Extract JPN rows from desktop table #jpn."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_target_table = False
        self._table_depth = 0

        self._in_row = False
        self._in_td = False
        self._td_index = 0
        self._current_td_text: list[str] = []

        self._row_subject = ""
        self._row_main_href: str | None = None
        self._row_accessible_href: str | None = None

        self.rows: list[_RowData] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = dict(attrs)

        if tag == "table":
            if not self._in_target_table and clean_text(str(attrs_map.get("id") or "")) == "jpn":
                self._in_target_table = True
                self._table_depth = 1
                return
            if self._in_target_table:
                self._table_depth += 1
                return

        if not self._in_target_table:
            return

        if tag == "tr":
            self._in_row = True
            self._in_td = False
            self._td_index = 0
            self._current_td_text = []
            self._row_subject = ""
            self._row_main_href = None
            self._row_accessible_href = None
            return

        if not self._in_row:
            return

        if tag == "td":
            self._in_td = True
            self._td_index += 1
            self._current_td_text = []
            return

        if tag == "a" and self._in_td:
            href = clean_text(str(attrs_map.get("href") or ""))
            if not href:
                return
            if self._td_index == 2 and self._row_main_href is None:
                self._row_main_href = href
            elif self._td_index == 3 and self._row_accessible_href is None:
                self._row_accessible_href = href

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._in_target_table:
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_target_table = False
                self._table_depth = 0
            return

        if not self._in_target_table:
            return

        if tag == "td" and self._in_td:
            if self._td_index == 2:
                self._row_subject = clean_text("".join(self._current_td_text))
            self._in_td = False
            self._current_td_text = []
            return

        if tag == "tr" and self._in_row:
            subject = clean_text(self._row_subject)
            if subject and (self._row_main_href or self._row_accessible_href):
                self.rows.append(
                    _RowData(
                        subject=subject,
                        main_href=self._row_main_href,
                        accessible_href=self._row_accessible_href,
                    )
                )

            self._in_row = False
            self._in_td = False
            self._td_index = 0
            self._current_td_text = []
            self._row_subject = ""
            self._row_main_href = None
            self._row_accessible_href = None

    def handle_data(self, data: str) -> None:
        if self._in_row and self._in_td and self._td_index == 2:
            self._current_td_text.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "joint_practice_notes"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))

        request_delay = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.10))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        resp = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        resp.encoding = "utf-8"

        parser = _JpnDesktopTableParser()
        parser.feed(resp.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for row in parser.rows:
            for raw_href in (row.main_href, row.accessible_href):
                if len(out) >= max_total_records:
                    break
                if not raw_href:
                    continue

                candidate_url = _canonicalize(urljoin(page_url, raw_href))
                if not candidate_url:
                    continue
                if path_ext(candidate_url) != ".pdf":
                    continue
                if candidate_url in seen_urls:
                    continue

                out.append(
                    ctx.make_record(
                        url=candidate_url,
                        name=row.subject,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={"discovered_from": page_url},
                    )
                )
                seen_urls.add(candidate_url)

            if len(out) >= max_total_records:
                break

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
