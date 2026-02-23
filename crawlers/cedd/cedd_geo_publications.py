from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import re
from urllib.parse import urlparse
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
)
from utils.html_links import extract_links, extract_links_in_element


@dataclass(frozen=True)
class _VisitItem:
    url: str
    discovered_from: str
    from_leaflets: bool


class _PageSignalsParser(HTMLParser):
    """Collect page title and detect if page has an Abstract section."""

    def __init__(self, *, content_element_id: str = "content") -> None:
        super().__init__()
        self._content_element_id = content_element_id
        self._content_depth = 0

        self._in_page_title = False
        self._page_title_depth = 0
        self._page_title_parts: list[str] = []

        self._in_heading = False
        self._heading_depth = 0
        self._heading_parts: list[str] = []
        self.headings: list[str] = []

        self._content_text_parts: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is not None:
                out[k.lower()] = v
        return out

    def _in_content(self) -> bool:
        return self._content_depth > 0

    @property
    def page_title(self) -> str | None:
        t = clean_text("".join(self._page_title_parts))
        return t or None

    @property
    def has_abstract_section(self) -> bool:
        for h in self.headings:
            normalized = clean_text(h).lower().rstrip(":")
            if normalized == "abstract" or normalized.startswith("abstract "):
                return True

        content_text = clean_text(" ".join(self._content_text_parts)).lower()
        return bool(re.search(r"\babstract\b", content_text))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if self._content_depth == 0 and attrs_map.get("id") == self._content_element_id:
            self._content_depth = 1
        elif self._content_depth > 0:
            self._content_depth += 1

        if self._in_page_title:
            self._page_title_depth += 1

        if attrs_map.get("id") == "pageTitle":
            self._in_page_title = True
            self._page_title_depth = 1
            self._page_title_parts = []

        if self._in_content() and t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._heading_depth = 1
            self._heading_parts = []
        elif self._in_heading:
            self._heading_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if self._in_heading and self._heading_depth > 0:
            self._heading_depth -= 1
            if self._heading_depth == 0:
                heading = clean_text("".join(self._heading_parts))
                if heading:
                    self.headings.append(heading)
                self._in_heading = False
                self._heading_parts = []

        if self._in_page_title and self._page_title_depth > 0:
            self._page_title_depth -= 1
            if self._page_title_depth == 0:
                self._in_page_title = False

        if self._content_depth > 0:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._in_page_title:
            self._page_title_parts.append(data)
        if self._in_heading:
            self._heading_parts.append(data)
        if self._in_content():
            self._content_text_parts.append(data)


def _clean_anchor_text(text: str | None) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    cleaned = re.sub(
        r"\bThis link will open in new window\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return clean_text(cleaned)


def _path_in_scope(url: str, *, host: str, scope_prefix: str) -> bool:
    p = urlparse(url)
    return p.netloc.lower() == host.lower() and (p.path or "").startswith(scope_prefix)


def _looks_like_html_page(url: str) -> bool:
    ext = path_ext(url)
    return ext in ("", ".html", ".htm")


def _is_subpage(parent_url: str, child_url: str) -> bool:
    parent_path = urlparse(parent_url).path or "/"
    child_path = urlparse(child_url).path or "/"

    if child_path == parent_path:
        return False

    parent_dir = parent_path.rsplit("/", 1)[0] + "/"
    return child_path.startswith(parent_dir)


class Crawler:
    """Crawl CEDD GEO publications recursively and emit PDF/page records."""

    name = "cedd_geo_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                f"{base_url}/eng/publications/geo/index.html",
            )
        ).strip()
        scope_prefix = str(cfg.get("scope_prefix", "/eng/publications/geo/")).strip()
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )
        max_pages = int(cfg.get("max_pages", 1200))
        max_out_links_per_page = int(cfg.get("max_out_links_per_page", 1200))
        leaflets_url = str(
            cfg.get(
                "leaflets_url",
                f"{base_url}{scope_prefix.rstrip('/')}/leaflets-and-brochures/index.html",
            )
        ).strip()

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen_pages: set[str] = set()
        seen_record_keys: set[tuple[str, str]] = set()

        seed_url = canonicalize_url(page_url, encode_spaces=True)
        leaflets_canonical = canonicalize_url(leaflets_url, encode_spaces=True)
        if not seed_url:
            return []

        seed_host = urlparse(seed_url).netloc.lower()
        queue: list[_VisitItem] = [
            _VisitItem(url=seed_url, discovered_from=seed_url, from_leaflets=False)
        ]

        def _append_record(
            *,
            record_url: str,
            title: str | None,
            discovered_from_url: str,
            record_type: str,
            abstract_page: bool = False,
            reason: str | None = None,
        ) -> None:
            if len(out) >= max_total_records:
                return
            key = (record_type, record_url)
            if key in seen_record_keys:
                return
            seen_record_keys.add(key)

            meta: dict[str, str | bool | None] = {
                "type": record_type,
                "title": title or None,
                "discovered_from": discovered_from_url,
            }
            if record_type == "page":
                meta["abstract_page"] = abstract_page
            if reason:
                meta["reason"] = reason

            out.append(
                UrlRecord(
                    url=record_url,
                    name=title or None,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta=meta,
                )
            )

        while queue and len(seen_pages) < max_pages and len(out) < max_total_records:
            item = queue.pop(0)
            current_url = item.url

            if current_url in seen_pages:
                continue
            seen_pages.add(current_url)

            if ctx.debug:
                print(f"[{self.name}] Fetch -> {current_url}")

            try:
                resp = get_with_retries(
                    session,
                    current_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except requests.RequestException:
                continue

            html = resp.text

            links = extract_links_in_element(
                html,
                base_url=current_url,
                element_id=content_element_id,
            )
            if not links:
                links = extract_links(html, base_url=current_url)
            if len(links) > max_out_links_per_page:
                links = links[:max_out_links_per_page]

            signals = _PageSignalsParser(content_element_id=content_element_id)
            signals.feed(html)
            has_abstract = signals.has_abstract_section
            page_title = signals.page_title or infer_name_from_link(None, current_url)

            direct_pdf_count = 0
            next_pages: list[str] = []

            for link in links:
                abs_url = canonicalize_url(
                    urljoin(current_url, link.href), encode_spaces=True
                )
                if not abs_url:
                    continue

                if path_ext(abs_url) == ".pdf":
                    if urlparse(abs_url).netloc.lower() != seed_host:
                        continue
                    direct_pdf_count += 1
                    title = _clean_anchor_text(link.text) or infer_name_from_link(
                        link.text, abs_url
                    )
                    _append_record(
                        record_url=abs_url,
                        title=title,
                        discovered_from_url=current_url,
                        record_type="pdf",
                    )
                    continue

                if not _looks_like_html_page(abs_url):
                    continue
                if not _path_in_scope(
                    abs_url, host=seed_host, scope_prefix=scope_prefix
                ):
                    continue
                if not _is_subpage(current_url, abs_url):
                    continue
                if abs_url in seen_pages:
                    continue
                next_pages.append(abs_url)

            if has_abstract:
                _append_record(
                    record_url=current_url,
                    title=page_title,
                    discovered_from_url=item.discovered_from,
                    record_type="page",
                    abstract_page=True,
                    reason="abstract",
                )
                continue

            if item.from_leaflets and direct_pdf_count == 0:
                _append_record(
                    record_url=current_url,
                    title=page_title,
                    discovered_from_url=item.discovered_from,
                    record_type="page",
                    abstract_page=False,
                    reason="leaflets_subpage_without_pdf",
                )

            for next_url in next_pages:
                queue.append(
                    _VisitItem(
                        url=next_url,
                        discovered_from=current_url,
                        from_leaflets=bool(
                            leaflets_canonical and current_url == leaflets_canonical
                        ),
                    )
                )

        out.sort(
            key=lambda r: (
                r.url,
                str(r.meta.get("type") or ""),
                str(r.meta.get("title") or ""),
            )
        )
        return out
