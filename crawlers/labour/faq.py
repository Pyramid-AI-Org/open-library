from __future__ import annotations

import random
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
    sleep_seconds,
)


_DEFAULT_PAGE_URL = "https://www.labour.gov.hk/eng/faq/content.htm"
_DEFAULT_ALLOWED_PATH_PREFIXES = ("/eng/faq/",)


class _FaqPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.entries: list[dict[str, str]] = []
        self.section_order: dict[str, int] = {}

        self._in_main_content = False
        self._main_content_depth = 0

        self._in_target_table = False
        self._target_table_depth = 0

        self._in_h2 = False
        self._h2_text_parts: list[str] = []
        self._current_section_name: str | None = None

        self._ul_depth = 0
        self._li_depth = 0

        self._in_a = False
        self._a_text_parts: list[str] = []
        self._a_href: str | None = None
        self._a_class: str = ""

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    def _remember_section(self, section_name: str) -> None:
        if section_name not in self.section_order:
            self.section_order[section_name] = len(self.section_order) + 1

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

        if t == "table":
            if not self._in_target_table:
                self._in_target_table = True
                self._target_table_depth = 1
            else:
                self._target_table_depth += 1
            return

        if not self._in_target_table:
            return

        if t == "h2":
            self._in_h2 = True
            self._h2_text_parts = []
            return

        if t == "ul" and self._current_section_name:
            self._ul_depth += 1
            return

        if t == "li" and self._ul_depth > 0:
            self._li_depth += 1
            return

        if t == "a" and self._li_depth > 0 and self._current_section_name:
            self._in_a = True
            self._a_text_parts = []
            self._a_href = attrs_map.get("href")
            self._a_class = attrs_map.get("class", "")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div" and self._in_main_content:
            self._main_content_depth -= 1
            if self._main_content_depth <= 0:
                self._in_main_content = False
                self._main_content_depth = 0
                self._in_target_table = False
                self._target_table_depth = 0

        if not self._in_main_content:
            return

        if t == "table" and self._in_target_table:
            self._target_table_depth -= 1
            if self._target_table_depth <= 0:
                self._in_target_table = False
                self._target_table_depth = 0
            return

        if not self._in_target_table:
            return

        if t == "h2" and self._in_h2:
            section_name = clean_text("".join(self._h2_text_parts))
            self._current_section_name = section_name or None
            if self._current_section_name:
                self._remember_section(self._current_section_name)
            self._in_h2 = False
            self._h2_text_parts = []
            return

        if t == "ul" and self._ul_depth > 0:
            self._ul_depth -= 1
            return

        if t == "li" and self._li_depth > 0:
            self._li_depth -= 1
            return

        if t == "a" and self._in_a:
            if self._current_section_name and self._a_href:
                self.entries.append(
                    {
                        "section_name": self._current_section_name,
                        "href": self._a_href,
                        "name": clean_text("".join(self._a_text_parts)),
                        "class_name": self._a_class,
                    }
                )
            self._in_a = False
            self._a_text_parts = []
            self._a_href = None
            self._a_class = ""

    def handle_data(self, data: str) -> None:
        if self._in_h2:
            self._h2_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "faq"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        allowed_path_prefixes = cfg.get("allowed_path_prefixes", _DEFAULT_ALLOWED_PATH_PREFIXES)
        if not isinstance(allowed_path_prefixes, list):
            allowed_path_prefixes = list(_DEFAULT_ALLOWED_PATH_PREFIXES)
        allowed_path_prefixes = [
            clean_text(str(prefix))
            for prefix in allowed_path_prefixes
            if clean_text(str(prefix))
        ]

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

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        response = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"

        parser = _FaqPageParser()
        parser.feed(response.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for entry in parser.entries:
            if len(out) >= max_total_records:
                break

            section_name = clean_text(entry.get("section_name", ""))
            href = clean_text(entry.get("href", ""))
            link_name = clean_text(entry.get("name", ""))
            class_name = clean_text(entry.get("class_name", "")).lower()

            if not section_name or not href:
                continue
            if "externalurl" in class_name:
                continue
            if link_name.lower() == "this link will open in a new window":
                continue

            canonical_url = _canonicalize(urljoin(page_url, href))
            if not canonical_url or canonical_url in seen_urls:
                continue

            path = (urlparse(canonical_url).path or "").strip()
            if allowed_path_prefixes and not any(
                path.startswith(prefix) for prefix in allowed_path_prefixes
            ):
                continue

            name = link_name or infer_name_from_link(link_name, canonical_url)

            meta: dict[str, str | int] = {
                "section_name": section_name,
                "section_index": parser.section_order.get(section_name, 0),
                "discovered_from": page_url,
            }

            out.append(
                ctx.make_record(
                    url=canonical_url,
                    name=name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta=meta,
                )
            )
            seen_urls.add(canonical_url)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.meta.get("section_name") or ""),
                str(r.meta.get("section_index") or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
