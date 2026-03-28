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
    path_ext,
    sleep_seconds,
)
from utils.html_links import HtmlLink, extract_links, extract_links_in_element


@dataclass(frozen=True)
class QueueItem:
    url: str
    discovered_from: str | None


@dataclass(frozen=True)
class TraversalMode:
    emit_page_records: bool
    emit_pdf_records: bool
    emit_pdf_from_seed: bool
    emit_pdf_from_subpages: bool
    include_seed_page_record: bool
    seed_page_discovered_from_self: bool


class _TitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_key: str | None = None
        self._capture_depth = 0
        self._text_parts: list[str] = []

        self.title: str | None = None
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

        if t == "title" and self.title is None:
            self._capture_key = "title"
            self._capture_depth = 1
            self._text_parts = []
            return

        if t == "h2" and attrs_map.get("id", "").strip().lower() == "page_title":
            self._capture_key = "page_title"
            self._capture_depth = 1
            self._text_parts = []
            return

        if t in {"h1", "h2"} and self.first_heading is None:
            self._capture_key = "first_heading"
            self._capture_depth = 1
            self._text_parts = []

    def handle_endtag(self, _tag: str) -> None:
        if self._capture_depth <= 0:
            return

        self._capture_depth -= 1
        if self._capture_depth > 0:
            return

        text = clean_text("".join(self._text_parts))
        if text:
            if self._capture_key == "title" and self.title is None:
                self.title = text
            elif self._capture_key == "page_title" and self.page_title is None:
                self.page_title = text
            elif self._capture_key == "first_heading" and self.first_heading is None:
                self.first_heading = text

        self._capture_key = None
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_depth > 0:
            self._text_parts.append(data)


def canonicalize_wsd_url(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def as_url_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        text = raw.strip()
        return [text] if text else []
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


def extract_links_scoped(
    html: str, *, base_url: str, content_element_id: str
) -> list[HtmlLink]:
    links = extract_links_in_element(
        html,
        base_url=base_url,
        element_id=content_element_id,
    )
    if not links:
        links = extract_links(html, base_url=base_url)
    return links


def extract_page_title(html: str) -> str | None:
    parser = _TitleParser()
    parser.feed(html)
    return parser.title or parser.page_title or parser.first_heading


def is_in_scope(url: str, *, scope_prefix: str) -> bool:
    return url == scope_prefix or url.startswith(scope_prefix + "/")


def is_subpage_url(url: str) -> bool:
    ext = path_ext(url)
    return ext in {"", ".html", ".htm"}


def sort_records(records: list[UrlRecord]) -> None:
    records.sort(
        key=lambda r: (
            r.url,
            str(r.name or ""),
            str(r.meta.get("discovered_from") or ""),
        )
    )


def crawl_page_tree(
    ctx: RunContext,
    *,
    source_name: str,
    page_url: str,
    scope_prefix: str,
    content_element_id: str,
    max_pages: int,
    max_out_links_per_page: int,
    max_total_records: int,
    request_delay: float,
    request_jitter: float,
    backoff_base: float,
    backoff_jitter: float,
    timeout_seconds: int,
    user_agent: str,
    max_retries: int,
    mode: TraversalMode,
    excluded_subpages: set[str] | None = None,
) -> list[UrlRecord]:
    seed_url = canonicalize_wsd_url(page_url)
    norm_scope_prefix = (scope_prefix or "").strip().rstrip("/")
    if not seed_url or not norm_scope_prefix:
        return []
    if not is_in_scope(seed_url, scope_prefix=norm_scope_prefix):
        return []

    blocked_subpages = excluded_subpages or set()

    session = requests.Session()
    if user_agent:
        session.headers.update({"User-Agent": user_agent})

    def _fetch(url: str) -> str:
        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))
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

    out: list[UrlRecord] = []
    seen_out_urls: set[str] = set()

    queue: list[QueueItem] = [QueueItem(url=seed_url, discovered_from=None)]
    enqueued_pages: set[str] = {seed_url}
    visited_pages: set[str] = set()

    while queue and len(visited_pages) < max_pages and len(out) < max_total_records:
        item = queue.pop(0)
        if item.url in visited_pages:
            continue
        visited_pages.add(item.url)
        is_seed = item.url == seed_url

        html = _fetch(item.url)

        if mode.emit_page_records and (mode.include_seed_page_record or not is_seed):
            discovered_from = item.discovered_from
            if is_seed and mode.seed_page_discovered_from_self:
                discovered_from = item.url
            if not discovered_from:
                discovered_from = item.url

            if item.url not in seen_out_urls:
                out.append(
                    ctx.make_record(
                        url=item.url,
                        name=extract_page_title(html),
                        discovered_at_utc=ctx.run_date_utc,
                        source=source_name,
                        meta={"discovered_from": discovered_from},
                    )
                )
                seen_out_urls.add(item.url)

        links = extract_links_scoped(
            html,
            base_url=item.url,
            content_element_id=content_element_id,
        )

        queued_on_this_page = 0
        for link in links:
            if len(out) >= max_total_records:
                break
            if queued_on_this_page >= max_out_links_per_page:
                break

            can = canonicalize_wsd_url(urljoin(item.url, link.href))
            if not can:
                continue

            if path_ext(can) == ".pdf":
                should_emit_pdf = mode.emit_pdf_records and (
                    (is_seed and mode.emit_pdf_from_seed)
                    or ((not is_seed) and mode.emit_pdf_from_subpages)
                )
                if not should_emit_pdf:
                    continue
                if can in seen_out_urls:
                    continue
                out.append(
                    ctx.make_record(
                        url=can,
                        name=clean_text(link.text)
                        or infer_name_from_link(link.text, can),
                        discovered_at_utc=ctx.run_date_utc,
                        source=source_name,
                        meta={"discovered_from": item.url},
                    )
                )
                seen_out_urls.add(can)
                continue

            if not is_subpage_url(can):
                continue
            if not is_in_scope(can, scope_prefix=norm_scope_prefix):
                continue
            if can in blocked_subpages:
                continue
            if can in visited_pages or can in enqueued_pages:
                continue

            queue.append(QueueItem(url=can, discovered_from=item.url))
            enqueued_pages.add(can)
            queued_on_this_page += 1

    sort_records(out)
    return out[:max_total_records]
