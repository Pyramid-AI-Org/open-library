from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import random
import re

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
from utils.html_links import HtmlLink, extract_links


_ALLOWED_PDF_EXT = ".pdf"
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


@dataclass(frozen=True)
class _Target:
    key: str
    page_url: str
    selection_mode: str
    include_text: str = ""


class _HeadingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_tag: str | None = None
        self._parts: list[str] = []
        self.h1: str = ""
        self.h2: str = ""
        self.title: str = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "h1" and not self.h1:
            self._capture_tag = "h1"
            self._parts = []
        elif t == "h2" and not self.h2:
            self._capture_tag = "h2"
            self._parts = []
        elif t == "title" and not self.title:
            self._capture_tag = "title"
            self._parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if self._capture_tag and t == self._capture_tag:
            text = clean_text("".join(self._parts))
            if self._capture_tag == "h1" and text:
                self.h1 = text
            elif self._capture_tag == "h2" and text:
                self.h2 = text
            elif self._capture_tag == "title" and text:
                self.title = text
            self._capture_tag = None
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_tag:
            self._parts.append(data)


def _parse_heading(html: str) -> str:
    parser = _HeadingParser()
    parser.feed(html or "")
    return clean_text(parser.h1 or parser.h2 or parser.title)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _extract_pdf_links(html: str, page_url: str) -> list[HtmlLink]:
    links = extract_links(html or "", base_url=page_url)
    out: list[HtmlLink] = []
    for link in links:
        can = _canonicalize(link.href)
        if not can:
            continue
        if path_ext(can) != _ALLOWED_PDF_EXT:
            continue
        out.append(HtmlLink(href=can, text=clean_text(link.text)))
    return out


def _extract_year(value: str) -> int | None:
    m = _YEAR_RE.search(value or "")
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def _pick_heading_text_match(links: list[HtmlLink], include_text: str) -> HtmlLink | None:
    token = clean_text(include_text).lower()
    if not token:
        return links[0] if links else None

    for link in links:
        if token in clean_text(link.text).lower():
            return link
    return links[0] if links else None


def _pick_latest_by_year(links: list[HtmlLink]) -> HtmlLink | None:
    best: tuple[int, int] | None = None
    best_link: HtmlLink | None = None

    for idx, link in enumerate(links):
        year = _extract_year(link.text) or _extract_year(link.href) or 0
        score = (year, -idx)
        if best is None or score > best:
            best = score
            best_link = link

    return best_link


def _pick_latest_subpage(root_html: str, root_url: str) -> str | None:
    links = extract_links(root_html or "", base_url=root_url)
    best: tuple[int, int] | None = None
    best_url: str | None = None

    for idx, link in enumerate(links):
        can = _canonicalize(link.href)
        if not can:
            continue

        lower = can.lower()
        if lower.endswith(".pdf"):
            continue

        year = _extract_year(link.text) or _extract_year(can)
        if year is None:
            continue

        if "annual traffic census" not in clean_text(link.text).lower() and "/atc" not in lower:
            continue

        score = (year, -idx)
        if best is None or score > best:
            best = score
            best_url = can

    return best_url


def _pick_atci2_main_pdf(links: list[HtmlLink]) -> HtmlLink | None:
    best: tuple[int, int] | None = None
    best_link: HtmlLink | None = None

    for idx, link in enumerate(links):
        text = clean_text(link.text).lower()
        href = (link.href or "").lower()

        if "amendment" in text or "amendment" in href:
            continue

        if "annual traffic census" not in text and "annual%20traffic%20census" not in href:
            continue

        year = _extract_year(link.text) or _extract_year(link.href) or 0
        score = (year, -idx)
        if best is None or score > best:
            best = score
            best_link = link

    if best_link is not None:
        return best_link

    # Fallback if wording changes: keep first non-amendment PDF.
    for link in links:
        text = clean_text(link.text).lower()
        href = (link.href or "").lower()
        if "amendment" in text or "amendment" in href:
            continue
        return link

    return None


class Crawler:
    name = "miscellaneous"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        targets_cfg = cfg.get("targets", [])
        targets: list[_Target] = []
        for item in targets_cfg:
            if not isinstance(item, dict):
                continue
            key = clean_text(str(item.get("key", "")))
            page_url = clean_text(str(item.get("page_url", "")))
            selection_mode = clean_text(str(item.get("selection_mode", "")))
            include_text = clean_text(str(item.get("include_text", "")))
            if not key or not page_url or not selection_mode:
                continue
            targets.append(
                _Target(
                    key=key,
                    page_url=page_url,
                    selection_mode=selection_mode,
                    include_text=include_text,
                )
            )

        if not targets:
            return []

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []

        for target in targets:
            try:
                if request_delay_seconds > 0:
                    sleep_seconds(
                        request_delay_seconds
                        + random.uniform(0.0, max(0.0, request_jitter_seconds))
                    )

                page_resp = get_with_retries(
                    session,
                    target.page_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                page_resp.encoding = "utf-8"
                page_html = page_resp.text or ""

                page_heading = _parse_heading(page_html)
                selected: HtmlLink | None = None
                discovered_from = target.page_url

                if target.selection_mode == "heading_text_match":
                    pdf_links = _extract_pdf_links(page_html, target.page_url)
                    selected = _pick_heading_text_match(pdf_links, target.include_text)

                elif target.selection_mode == "latest_by_year_from_link_text":
                    pdf_links = _extract_pdf_links(page_html, target.page_url)
                    selected = _pick_latest_by_year(pdf_links)

                elif target.selection_mode == "latest_subpage_then_main_pdf":
                    subpage_url = _pick_latest_subpage(page_html, target.page_url)
                    if subpage_url:
                        discovered_from = subpage_url
                        subpage_resp = get_with_retries(
                            session,
                            subpage_url,
                            timeout_seconds=timeout_seconds,
                            max_retries=max_retries,
                            backoff_base_seconds=backoff_base_seconds,
                            backoff_jitter_seconds=backoff_jitter_seconds,
                        )
                        subpage_resp.encoding = "utf-8"
                        subpage_html = subpage_resp.text or ""
                        subpage_pdf_links = _extract_pdf_links(subpage_html, subpage_url)
                        selected = _pick_atci2_main_pdf(subpage_pdf_links)

                if not selected:
                    if ctx.debug:
                        print(f"[{self.name}] No match for target {target.key}")
                    continue

                out.append(
                    ctx.make_record(
                        url=selected.href,
                        name=page_heading or target.key,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={
                            "discovered_from": discovered_from
                        },
                    )
                )
            except Exception as exc:
                if ctx.debug:
                    print(f"[{self.name}] Error processing {target.key}: {exc}")
                continue

        out.sort(
            key=lambda r: next(
                (
                    idx
                    for idx, target in enumerate(targets)
                    if target.key == str(r.meta.get("target_key") or "")
                ),
                10_000,
            )
        )
        return out
