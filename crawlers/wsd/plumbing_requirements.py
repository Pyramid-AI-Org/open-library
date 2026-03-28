from __future__ import annotations

import random
import re
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
    path_ext,
    sleep_seconds,
)
from utils.html_links import HtmlLink, extract_links, extract_links_in_element


_DEFAULT_PAGE_URL = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/"
)
_DEFAULT_LATEST_VERSION_SUBPAGES = [
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/"
    "technical-requirements-for-plumbing-works-in-bldgs/index.html",
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/"
    "guide-to-application-for-water-supply/index.html",
]
_DEFAULT_PREVIOUS_DOCS_SUBPAGE = (
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/"
    "previously-used-documents-hkwsr-hpi/index.html"
)
_DEFAULT_EXCLUDE_SUBPAGES = [
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/wsd-circular-letters/index.html",
    "https://www.wsd.gov.hk/en/plumbing-engineering/"
    "requirements-for-plumbing-installation/common-observations-in-plumbing-proposals/index.html",
]


class _PageTitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_depth = 0
        self._capture_key: str | None = None
        self._text_parts: list[str] = []

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

        if t == "h2" and attrs_map.get("id", "").strip().lower() == "page_title":
            self._capture_depth = 1
            self._capture_key = "page_title"
            self._text_parts = []
            return

        if t in {"h1", "h2"} and self.first_heading is None:
            self._capture_depth = 1
            self._capture_key = "first_heading"
            self._text_parts = []

    def handle_endtag(self, _tag: str) -> None:
        if self._capture_depth <= 0:
            return

        self._capture_depth -= 1
        if self._capture_depth > 0:
            return

        text = clean_text("".join(self._text_parts))
        if text:
            if self._capture_key == "page_title":
                self.page_title = text
            elif self._capture_key == "first_heading" and self.first_heading is None:
                self.first_heading = text

        self._capture_key = None
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_depth > 0:
            self._text_parts.append(data)


class _LatestVersionFirstPdfParser(HTMLParser):
    def __init__(self, *, content_element_id: str) -> None:
        super().__init__()
        self.content_element_id = content_element_id.strip().lower() or "content"

        self._in_content = False
        self._content_depth = 0

        self._in_heading = False
        self._heading_text_parts: list[str] = []

        self._in_a = False
        self._a_href: str | None = None

        self._latest_section_active = False
        self.first_latest_href: str | None = None

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

        if not self._in_content:
            if attrs_map.get("id", "").strip().lower() == self.content_element_id:
                self._in_content = True
                self._content_depth = 1
            return

        self._content_depth += 1

        if t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._heading_text_parts = []
            if self._latest_section_active:
                self._latest_section_active = False
            return

        if t == "a" and self._latest_section_active and self.first_latest_href is None:
            self._in_a = True
            self._a_href = attrs_map.get("href")

    def handle_endtag(self, tag: str) -> None:
        if not self._in_content:
            return

        t = tag.lower()

        if self._in_heading and t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            heading_text = clean_text("".join(self._heading_text_parts)).lower()
            if "latest version" in heading_text:
                self._latest_section_active = True
            self._in_heading = False
            self._heading_text_parts = []

        if t == "a" and self._in_a:
            if self._a_href and self.first_latest_href is None:
                self.first_latest_href = self._a_href
            self._in_a = False
            self._a_href = None

        self._content_depth -= 1
        if self._content_depth <= 0:
            self._in_content = False
            self._content_depth = 0

    def handle_data(self, data: str) -> None:
        if self._in_heading:
            self._heading_text_parts.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _as_url_list(raw: object) -> list[str]:
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


def _extract_links_scoped(
    html: str, *, base_url: str, content_element_id: str
) -> list[HtmlLink]:
    links = extract_links_in_element(
        html, base_url=base_url, element_id=content_element_id
    )
    if not links:
        links = extract_links(html, base_url=base_url)
    return links


def _extract_page_title(html: str) -> str | None:
    parser = _PageTitleParser()
    parser.feed(html)
    return parser.page_title or parser.first_heading


def _iter_pdf_links(
    links: list[HtmlLink], *, base_url: str
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    for link in links:
        can = _canonicalize(urljoin(base_url, link.href))
        if not can or can in seen:
            continue
        if path_ext(can) != ".pdf":
            continue

        out.append((can, clean_text(link.text)))
        seen.add(can)

    return out


def _first_latest_version_pdf_url(
    html: str,
    *,
    base_url: str,
    content_element_id: str,
) -> str | None:
    parser = _LatestVersionFirstPdfParser(content_element_id=content_element_id)
    parser.feed(html)

    href = parser.first_latest_href
    if href:
        can = _canonicalize(urljoin(base_url, href))
        if can and path_ext(can) == ".pdf":
            return can

    pdf_links = _iter_pdf_links(
        _extract_links_scoped(html, base_url=base_url, content_element_id=content_element_id),
        base_url=base_url,
    )
    if pdf_links:
        return pdf_links[0][0]

    return None


def _score_latest_only_candidate(
    *,
    link_text: str,
    page_title: str,
) -> tuple[int, int, int, int]:
    text = clean_text(link_text).lower()
    title = clean_text(page_title).lower()

    has_latest_version = int("latest" in text and "version" in text)
    has_incorporating_amendments = int("incorpor" in text and "amend" in text)
    has_amendments = int("amend" in text)
    mentions_title = int(bool(title) and title in text)

    return (
        has_latest_version,
        has_incorporating_amendments,
        mentions_title,
        1 - has_amendments,
    )


def _pick_latest_only_pdf(
    html: str,
    *,
    subpage_url: str,
    content_element_id: str,
    page_title: str,
) -> tuple[str, str | None] | None:
    pdf_links = _iter_pdf_links(
        _extract_links_scoped(
            html,
            base_url=subpage_url,
            content_element_id=content_element_id,
        ),
        base_url=subpage_url,
    )
    if not pdf_links:
        return None

    best_idx = 0
    best_score = (-1, -1, -1, -1)
    for idx, (_url, text) in enumerate(pdf_links):
        score = _score_latest_only_candidate(link_text=text, page_title=page_title)
        if score > best_score:
            best_score = score
            best_idx = idx

    picked_url, picked_text = pdf_links[best_idx]
    return (picked_url, picked_text)


def _normalize_subpage(url: str, *, scope_prefix: str) -> str | None:
    can = _canonicalize(url)
    if not can:
        return None
    if not can.startswith(scope_prefix):
        return None
    if path_ext(can) == ".pdf":
        return None
    return can


def _extract_publish_date(name: str | None) -> str | None:
    text = clean_text(name)
    if not text:
        return None

    # Covers labels such as "(Jan 2025)" and other month-year mentions.
    token_match = re.search(
        r"\(([^)]+)\)", text
    )
    if token_match:
        parsed = normalize_publish_date(token_match.group(1))
        if parsed:
            return parsed

    return normalize_publish_date(text)


class Crawler:
    name = "plumbing_requirements"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = str(cfg.get("page_url", _DEFAULT_PAGE_URL)).strip()
        scope_prefix = (
            str(cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip().rstrip("/")
            + "/"
        )
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )
        max_total_records = int(cfg.get("max_total_records", 50000))

        latest_version_subpages_raw = _as_url_list(
            cfg.get("latest_version_subpages", _DEFAULT_LATEST_VERSION_SUBPAGES)
        )
        previous_docs_subpage_raw = str(
            cfg.get("previous_docs_subpage", _DEFAULT_PREVIOUS_DOCS_SUBPAGE)
        ).strip()
        exclude_subpages_raw = _as_url_list(
            cfg.get("exclude_subpages", _DEFAULT_EXCLUDE_SUBPAGES)
        )

        request_delay = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        def _fetch(url: str) -> str:
            if request_delay > 0:
                sleep_seconds(
                    request_delay + random.uniform(0.0, max(0.0, request_jitter))
                )
            resp = get_with_retries(
                session,
                url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base,
                backoff_jitter_seconds=backoff_jitter,
            )
            resp.encoding = "utf-8"
            return resp.text or ""

        latest_version_subpages: list[str] = []
        seen_latest_subpages: set[str] = set()
        for raw_url in latest_version_subpages_raw:
            can = _normalize_subpage(raw_url, scope_prefix=scope_prefix)
            if not can or can in seen_latest_subpages:
                continue
            latest_version_subpages.append(can)
            seen_latest_subpages.add(can)

        previous_docs_subpage = _normalize_subpage(
            previous_docs_subpage_raw, scope_prefix=scope_prefix
        )

        excluded_subpages: set[str] = set()
        for raw_url in exclude_subpages_raw:
            can = _normalize_subpage(raw_url, scope_prefix=scope_prefix)
            if can:
                excluded_subpages.add(can)

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        landing_html = _fetch(page_url)
        landing_links = _extract_links_scoped(
            landing_html,
            base_url=page_url,
            content_element_id=content_element_id,
        )

        for pdf_url, link_text in _iter_pdf_links(landing_links, base_url=page_url):
            if len(out) >= max_total_records:
                break
            if pdf_url in seen_pdf_urls:
                continue

            out.append(
                ctx.make_record(
                    url=pdf_url,
                    name=link_text or infer_name_from_link(link_text, pdf_url),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": page_url},
                )
            )
            seen_pdf_urls.add(pdf_url)

        for subpage_url in latest_version_subpages:
            if len(out) >= max_total_records:
                break
            if subpage_url in excluded_subpages:
                continue

            html = _fetch(subpage_url)
            pdf_url = _first_latest_version_pdf_url(
                html,
                base_url=subpage_url,
                content_element_id=content_element_id,
            )
            if not pdf_url or pdf_url in seen_pdf_urls:
                continue

            scoped_links = _extract_links_scoped(
                html,
                base_url=subpage_url,
                content_element_id=content_element_id,
            )
            name = None
            for can, text in _iter_pdf_links(scoped_links, base_url=subpage_url):
                if can == pdf_url:
                    name = text
                    break
            publish_date = _extract_publish_date(name)

            out.append(
                ctx.make_record(
                    url=pdf_url,
                    name=name or infer_name_from_link(name or "", pdf_url),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": subpage_url},
                    publish_date=publish_date,
                )
            )
            seen_pdf_urls.add(pdf_url)

        if previous_docs_subpage and previous_docs_subpage not in excluded_subpages:
            previous_html = _fetch(previous_docs_subpage)
            previous_links = _extract_links_scoped(
                previous_html,
                base_url=previous_docs_subpage,
                content_element_id=content_element_id,
            )

            nested_subpages: list[str] = []
            seen_nested_subpages: set[str] = set()

            for link in previous_links:
                can = _canonicalize(urljoin(previous_docs_subpage, link.href))
                if not can:
                    continue

                if path_ext(can) == ".pdf":
                    if can in seen_pdf_urls or len(out) >= max_total_records:
                        continue
                    out.append(
                        ctx.make_record(
                            url=can,
                            name=clean_text(link.text)
                            or infer_name_from_link(link.text, can),
                            discovered_at_utc=ctx.run_date_utc,
                            source=self.name,
                            meta={"discovered_from": previous_docs_subpage},
                        )
                    )
                    seen_pdf_urls.add(can)
                    continue

                subpage = _normalize_subpage(can, scope_prefix=scope_prefix)
                if not subpage:
                    continue
                if subpage in excluded_subpages:
                    continue
                if subpage in seen_nested_subpages:
                    continue
                seen_nested_subpages.add(subpage)
                nested_subpages.append(subpage)

            for nested_subpage in nested_subpages:
                if len(out) >= max_total_records:
                    break

                nested_html = _fetch(nested_subpage)
                page_title = _extract_page_title(nested_html) or ""
                latest = _pick_latest_only_pdf(
                    nested_html,
                    subpage_url=nested_subpage,
                    content_element_id=content_element_id,
                    page_title=page_title,
                )
                if latest is None:
                    continue

                pdf_url, picked_text = latest
                if pdf_url in seen_pdf_urls:
                    continue

                out.append(
                    ctx.make_record(
                        url=pdf_url,
                        name=picked_text or infer_name_from_link(picked_text, pdf_url),
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={"discovered_from": nested_subpage},
                    )
                )
                seen_pdf_urls.add(pdf_url)

        out.sort(
            key=lambda r: (
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out[:max_total_records]
