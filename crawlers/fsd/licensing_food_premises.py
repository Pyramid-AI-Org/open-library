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


_DEFAULT_PAGE_URL = (
    "https://www.hkfsd.gov.hk/eng/fire_protection/licensing/premises_food.html"
)
_DEFAULT_PDF_SCOPE_PREFIX = "https://www.hkfsd.gov.hk/"

_PREVIOUS_REVISIONS_HEADING_PREFIX = "previous revisions"


@dataclass(frozen=True)
class _PdfLink:
    href: str
    name: str
    section: str


class _FoodPremisesParser(HTMLParser):
    """Extracts named PDF links from the food premises licensing page.

    The page has two shapes of content inside <div class="content">:
      - A real <table> whose first <td> per row names the premises type
        (Restaurant, Factory Canteen, ...); that name becomes the section.
      - <h2>-headed lists after the table (Important Advice, Other
        Requirements, Previous Revisions, ...); the heading becomes the
        section.

    Checklist links share an <li> with the requirement they belong to, so
    links after the first PDF link in an <li> are prefixed with that first
    link's name to keep them distinguishable.
    """

    def __init__(self) -> None:
        super().__init__()
        self.links: list[_PdfLink] = []

        self._in_content = False
        self._content_depth = 0

        self._current_heading = ""

        self._in_table = False
        self._in_row = False
        self._td_index = -1
        self._row_label_parts: list[str] = []
        self._row_label = ""

        self._li_depth = 0
        self._li_main_name = ""

        self._in_a = False
        self._a_href = ""
        self._a_text_parts: list[str] = []

        self._in_h2 = False
        self._h2_text_parts: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            out[key.lower()] = value
        return out

    @staticmethod
    def _classes(value: str) -> set[str]:
        return {part.strip().lower() for part in value.split() if part.strip()}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            if self._in_content:
                self._content_depth += 1
            elif "content" in self._classes(attrs_map.get("class", "")):
                self._in_content = True
                self._content_depth = 1
            return

        if not self._in_content:
            return

        if t == "h2":
            self._in_h2 = True
            self._h2_text_parts = []
            return

        if t == "table":
            self._in_table = True
            return

        if t == "tr" and self._in_table:
            self._in_row = True
            self._td_index = -1
            self._row_label_parts = []
            self._row_label = ""
            return

        if t == "td" and self._in_row:
            self._td_index += 1
            return

        if t == "li":
            self._li_depth += 1
            if self._li_depth == 1:
                self._li_main_name = ""
            return

        if t == "a":
            href = clean_text(str(attrs_map.get("href") or ""))
            if href:
                self._in_a = True
                self._a_href = href
                self._a_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "div":
            if self._in_content:
                self._content_depth -= 1
                if self._content_depth <= 0:
                    self._in_content = False
                    self._content_depth = 0
            return

        if not self._in_content:
            return

        if t == "h2" and self._in_h2:
            self._current_heading = clean_text("".join(self._h2_text_parts))
            self._in_h2 = False
            self._h2_text_parts = []
            return

        if t == "table":
            self._in_table = False
            return

        if t == "tr":
            self._in_row = False
            self._td_index = -1
            self._row_label = ""
            return

        if t == "li":
            self._li_depth = max(0, self._li_depth - 1)
            if self._li_depth == 0:
                self._li_main_name = ""
            return

        if t == "a" and self._in_a:
            self._finish_link()

    def _finish_link(self) -> None:
        text = clean_text("".join(self._a_text_parts))
        href = self._a_href
        self._in_a = False
        self._a_href = ""
        self._a_text_parts = []

        if not href or not text:
            return

        is_pdf = href.lower().endswith(".pdf")

        name = text
        if self._li_depth > 0 and is_pdf:
            if not self._li_main_name:
                self._li_main_name = text
            elif text != self._li_main_name:
                name = f"{self._li_main_name} - {text}"

        if not is_pdf:
            return

        if self._in_row and self._row_label:
            section = self._row_label
        else:
            section = self._current_heading

        self.links.append(_PdfLink(href=href, name=name, section=section))

    def handle_data(self, data: str) -> None:
        if self._in_h2:
            self._h2_text_parts.append(data)
        if self._in_a:
            self._a_text_parts.append(data)
        if self._in_row and self._td_index == 0 and not self._row_label:
            self._row_label_parts.append(data)
            label = clean_text("".join(self._row_label_parts))
            if label:
                self._row_label = label


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "licensing_food_premises"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        pdf_scope_prefix = str(
            cfg.get("pdf_scope_prefix", _DEFAULT_PDF_SCOPE_PREFIX)
        ).strip()
        include_previous_revisions = bool(cfg.get("include_previous_revisions", False))

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
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

        response = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        response.encoding = "utf-8"

        parser = _FoodPremisesParser()
        parser.feed(response.text or "")

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for link in parser.links:
            if len(out) >= max_total_records:
                break

            section = clean_text(link.section)
            if not include_previous_revisions and section.lower().startswith(
                _PREVIOUS_REVISIONS_HEADING_PREFIX
            ):
                continue

            candidate = _canonicalize(urljoin(page_url, link.href))
            if not candidate:
                continue
            if path_ext(candidate) != ".pdf":
                continue
            if pdf_scope_prefix and not candidate.startswith(pdf_scope_prefix):
                continue
            if candidate in seen_urls:
                continue

            out.append(
                ctx.make_record(
                    url=candidate,
                    name=link.name,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={
                        "discovered_from": page_url,
                        "section": section,
                    },
                    publish_date=None,
                )
            )
            seen_urls.add(candidate)

        out.sort(
            key=lambda r: (
                str(r.meta.get("section") or ""),
                r.url,
                str(r.name or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
