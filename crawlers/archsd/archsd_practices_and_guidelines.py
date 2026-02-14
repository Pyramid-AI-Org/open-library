from __future__ import annotations

import random
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin, urlparse

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
from utils.html_links import HtmlLink, extract_links, extract_links_in_element

_ALLOWED_DOC_EXTS = {".pdf"}

_PDF_URL_RE = re.compile(
    r"((?:https?://|/)[^\"\'<>\s]+?\.pdf(?:\?[^\"\'<>\s]*)?)",
    re.IGNORECASE,
)


_clean_text = clean_text


def _is_generic_link_text(s: str) -> bool:
    t = _clean_text(s).lower()
    if not t:
        return True
    return t in {
        "more",
        "download",
        "view",
        "open",
        "pdf",
        "link",
        "here",
        "click here",
    }


def _title_from_aria_label(label: str) -> str | None:
    t = _clean_text(label)
    if not t:
        return None
    lower = t.lower()
    for prefix in ("go to ", "go to", "goto ", "goto"):
        if lower.startswith(prefix):
            t = _clean_text(t[len(prefix) :])
            break
    return t or None


class _PdfTitleParser(HTMLParser):
    """Best-effort mapping from PDF hrefs to human titles.

    Covers common ARCHSD patterns:
    - Cards: div.info-card ... div.title + a.btn (text='More')
    - Lists: div.list-item ... div.item-name + a.icon-link (no text)
    """

    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self._base_url = base_url
        self.pdf_url_to_title: dict[str, str] = {}

        self._card_depth = 0
        self._card_title: str | None = None

        self._list_item_depth = 0
        self._item_name: str | None = None

        self._capture_card_title_depth = 0
        self._card_title_parts: list[str] = []

        self._capture_item_name_depth = 0
        self._item_name_parts: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_list(attrs_map: dict[str, str]) -> set[str]:
        raw = attrs_map.get("class", "")
        return {c.strip() for c in raw.split() if c.strip()}

    def _maybe_record_pdf(self, href: str | None, *, aria_label: str | None) -> None:
        if not href:
            return

        full = urljoin(self._base_url, href)
        if _path_ext(full) != ".pdf":
            return

        title: str | None = None
        if self._list_item_depth > 0 and self._item_name:
            title = self._item_name
        elif self._card_depth > 0 and self._card_title:
            title = self._card_title
        else:
            title = _title_from_aria_label(aria_label or "")

        if not title:
            return

        can = _canonicalize_url(full)
        if not can:
            return
        self.pdf_url_to_title.setdefault(can, title)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = self._attrs_to_dict(attrs)
        classes = self._class_list(attrs_map)

        # If we're already inside a container, increase depth for any nested tag.
        if self._card_depth > 0:
            self._card_depth += 1
        if self._list_item_depth > 0:
            self._list_item_depth += 1

        if tag.lower() == "div":
            if self._card_depth == 0 and "info-card" in classes:
                self._card_depth = 1
                self._card_title = None

            if self._list_item_depth == 0 and "list-item" in classes:
                self._list_item_depth = 1
                self._item_name = None

            if (
                self._card_depth > 0
                and self._capture_card_title_depth == 0
                and "title" in classes
            ):
                self._capture_card_title_depth = 1
                self._card_title_parts = []

            if (
                self._list_item_depth > 0
                and self._capture_item_name_depth == 0
                and "item-name" in classes
            ):
                self._capture_item_name_depth = 1
                self._item_name_parts = []

        # Count nested tags inside title/name blocks so we can close correctly.
        if self._capture_card_title_depth > 0 and not (
            tag.lower() == "div"
            and "title" in classes
            and self._capture_card_title_depth == 1
        ):
            self._capture_card_title_depth += 1

        if self._capture_item_name_depth > 0 and not (
            tag.lower() == "div"
            and "item-name" in classes
            and self._capture_item_name_depth == 1
        ):
            self._capture_item_name_depth += 1

        if tag.lower() == "a":
            self._maybe_record_pdf(
                attrs_map.get("href"),
                aria_label=attrs_map.get("aria-label"),
            )

    def handle_endtag(self, tag: str) -> None:
        if self._capture_card_title_depth > 0:
            self._capture_card_title_depth -= 1
            if self._capture_card_title_depth == 0:
                t = _clean_text("".join(self._card_title_parts))
                if t:
                    self._card_title = t
                self._card_title_parts = []

        if self._capture_item_name_depth > 0:
            self._capture_item_name_depth -= 1
            if self._capture_item_name_depth == 0:
                t = _clean_text("".join(self._item_name_parts))
                if t:
                    self._item_name = t
                self._item_name_parts = []

        if self._card_depth > 0:
            self._card_depth -= 1
            if self._card_depth == 0:
                self._card_title = None

        if self._list_item_depth > 0:
            self._list_item_depth -= 1
            if self._list_item_depth == 0:
                self._item_name = None

    def handle_data(self, data: str) -> None:
        if self._capture_card_title_depth > 0:
            self._card_title_parts.append(data)
        if self._capture_item_name_depth > 0:
            self._item_name_parts.append(data)


def _extract_pdf_url_to_title(html: str, *, base_url: str) -> dict[str, str]:
    parser = _PdfTitleParser(base_url=base_url)
    parser.feed(html or "")
    out: dict[str, str] = {}
    for k, v in parser.pdf_url_to_title.items():
        t = _clean_text(v)
        if t:
            out[k] = t
    return out


_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries
_canonicalize_url = canonicalize_url
_path_ext = path_ext


def _iter_links(
    html: str, *, base_url: str, content_element_id: str
) -> Iterable[HtmlLink]:
    scoped = extract_links_in_element(
        html, base_url=base_url, element_id=content_element_id
    )
    if scoped:
        return scoped
    return extract_links(html, base_url)


def _path_starts_with_any(path: str, prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    p = path or "/"
    for pref in prefixes:
        if not pref:
            continue
        if p.startswith(pref):
            return True
    return False


def _path_is_explicitly_allowed(path: str, *, allowed_paths: set[str]) -> bool:
    p = path or "/"
    if p in allowed_paths:
        return True
    if not p.startswith("/") and ("/" + p) in allowed_paths:
        return True
    return False


def _extract_pdf_urls_from_html(html: str, *, base_url: str) -> list[str]:
    out: list[str] = []
    for m in _PDF_URL_RE.findall(html or ""):
        s = (m or "").strip()
        if not s:
            continue
        s = s.replace("&amp;", "&")
        s = s.rstrip(")].,;\"'\u00bb\u2019\u201d")

        if s.startswith("/"):
            s = urljoin(base_url, s)
        out.append(s)

    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


@dataclass(frozen=True)
class _QueueItem:
    url: str
    depth: int
    discovered_from: str | None


class Crawler:
    """ARCHSD Practices and Guidelines crawler."""

    name = "archsd_practices_and_guidelines"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.archsd.gov.hk")).rstrip("/")
        start_url = str(
            cfg.get("start_url", f"{base_url}/en/reports/practices-and-guidelines.html")
        ).strip()

        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        # Page scoping:
        # - Explicit allowlist for known entry pages (prevents crawling other /en/reports/* pages via nav)
        # - A small set of allowed subtrees for the intended subsections
        explicit_allowed_page_paths_raw = cfg.get("explicit_allowed_page_paths", None)
        explicit_allowed_page_paths: set[str] = set()
        if isinstance(explicit_allowed_page_paths_raw, list):
            for v in explicit_allowed_page_paths_raw:
                if not isinstance(v, str):
                    continue
                s = v.strip()
                if s:
                    explicit_allowed_page_paths.add(s)

        allowed_page_path_prefixes_raw = cfg.get("allowed_page_path_prefixes", None)
        if isinstance(allowed_page_path_prefixes_raw, list):
            allowed_page_path_prefixes = [
                str(v).strip()
                for v in allowed_page_path_prefixes_raw
                if isinstance(v, str) and str(v).strip()
            ]
        else:
            allowed_page_path_prefixes = [
                "/en/ua/",
                "/en/ua2/",
                "/en/thepossiblepackage/",
                "/en/BIM-guides/",
            ]

        allowed_pdf_path_prefixes_raw = cfg.get("allowed_pdf_path_prefixes", None)
        if isinstance(allowed_pdf_path_prefixes_raw, list):
            allowed_pdf_path_prefixes = [
                str(v).strip()
                for v in allowed_pdf_path_prefixes_raw
                if isinstance(v, str)
            ]
        else:
            allowed_pdf_path_prefixes = [
                "/media/reports/",
                "/archsd/html/ua/",
                "/archsd/html/ua2/",
                "/media/consultants-contractors/",
            ]

        max_depth = int(cfg.get("max_depth", 4))
        max_pages = int(cfg.get("max_pages", 500))
        max_out_links_per_page = int(cfg.get("max_out_links_per_page", 800))

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.10))

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        base_netloc = urlparse(base_url).netloc.lower()

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        start_can = _canonicalize_url(start_url)
        if not start_can:
            return []

        start_path = urlparse(start_can).path or "/"
        explicit_allowed_paths: set[str] = set(explicit_allowed_page_paths)
        explicit_allowed_paths.add(start_path)

        # Common explicitly-linked leaf pages from the hub.
        explicit_allowed_paths.update(
            {
                "/en/reports/disclaimer-for-BIM-guides.html",
                "/en/BIM-guides/index.html",
                "/en/consultants-contractors/product-conformity-certification-schemes.html",
            }
        )

        visited_pages: set[str] = set()
        skipped_pages: set[str] = set()
        seen_docs: set[str] = set()
        out: list[UrlRecord] = []

        queue: list[_QueueItem] = [
            _QueueItem(url=start_can, depth=0, discovered_from=None)
        ]

        while queue:
            item = queue.pop(0)

            if item.url in visited_pages or item.url in skipped_pages:
                continue
            if len(visited_pages) >= max_pages:
                break

            p = urlparse(item.url)
            if p.netloc.lower() != base_netloc:
                continue

            if not (
                _path_is_explicitly_allowed(
                    p.path, allowed_paths=explicit_allowed_paths
                )
                or _path_starts_with_any(p.path, allowed_page_path_prefixes)
            ):
                skipped_pages.add(item.url)
                continue

            visited_pages.add(item.url)

            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            if ctx.debug:
                print(f"[{self.name}] Fetch(depth={item.depth}) -> {item.url}")

            resp = _get_with_retries(
                session,
                item.url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )

            pdf_url_to_title = _extract_pdf_url_to_title(resp.text, base_url=item.url)

            links = list(
                _iter_links(
                    resp.text, base_url=item.url, content_element_id=content_element_id
                )
            )
            if max_out_links_per_page > 0:
                links = links[:max_out_links_per_page]

            pdf_urls_in_html = _extract_pdf_urls_from_html(resp.text, base_url=item.url)

            doc_url_to_text: dict[str, str] = {}
            for link in links:
                can = _canonicalize_url(link.href)
                if not can:
                    continue
                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue
                txt = link.text or ""
                if _is_generic_link_text(txt):
                    txt = pdf_url_to_title.get(can, "")
                doc_url_to_text[can] = txt

            for u in pdf_urls_in_html:
                can = _canonicalize_url(u)
                if not can:
                    continue
                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue
                if can not in doc_url_to_text:
                    doc_url_to_text[can] = pdf_url_to_title.get(can, "")

            for can in sorted(doc_url_to_text.keys()):
                lp = urlparse(can)
                if lp.netloc.lower() != base_netloc:
                    continue
                if allowed_pdf_path_prefixes and not _path_starts_with_any(
                    lp.path, allowed_pdf_path_prefixes
                ):
                    continue
                if can in seen_docs:
                    continue
                seen_docs.add(can)

                out.append(
                    UrlRecord(
                        url=can,
                        name=(doc_url_to_text.get(can, "") or None),
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "start_url": start_can,
                            "discovered_from": item.url,
                            "depth": item.depth,
                            "file_ext": "pdf",
                        },
                    )
                )

                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

            if item.depth >= max_depth:
                continue

            for link in links:
                can = _canonicalize_url(link.href)
                if not can:
                    continue
                if _path_ext(can) in _ALLOWED_DOC_EXTS:
                    continue

                lp = urlparse(can)
                if lp.netloc.lower() != base_netloc:
                    continue

                if not (
                    _path_is_explicitly_allowed(
                        lp.path, allowed_paths=explicit_allowed_paths
                    )
                    or _path_starts_with_any(lp.path, allowed_page_path_prefixes)
                ):
                    continue

                if can not in visited_pages:
                    queue.append(
                        _QueueItem(
                            url=can,
                            depth=item.depth + 1,
                            discovered_from=item.url,
                        )
                    )

        out.sort(key=lambda r: (r.url or ""))
        return out
