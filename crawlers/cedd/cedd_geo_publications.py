from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import html as html_lib
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

        self._in_html_title = False
        self._html_title_parts: list[str] = []

        self._in_page_title = False
        self._page_title_depth = 0
        self._page_title_parts: list[str] = []

        self._in_h1 = False
        self._h1_depth = 0
        self._h1_parts: list[str] = []
        self._first_h1: str | None = None

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
    def html_title(self) -> str | None:
        t = clean_text("".join(self._html_title_parts))
        return t or None

    @property
    def first_h1(self) -> str | None:
        return self._first_h1

    @property
    def content_text(self) -> str:
        return clean_text(" ".join(self._content_text_parts))

    @property
    def preferred_page_name(self) -> str | None:
        return self.first_h1 or self.page_title or self.html_title

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

        if t == "title":
            self._in_html_title = True
            self._html_title_parts = []

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

        if self._in_content() and t == "h1":
            self._in_h1 = True
            self._h1_depth = 1
            self._h1_parts = []
        elif self._in_h1:
            self._h1_depth += 1

        if self._in_content() and t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._heading_depth = 1
            self._heading_parts = []
        elif self._in_heading:
            self._heading_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if self._in_html_title and tag.lower() == "title":
            self._in_html_title = False

        if self._in_h1 and self._h1_depth > 0:
            self._h1_depth -= 1
            if self._h1_depth == 0:
                h1_text = clean_text("".join(self._h1_parts))
                if h1_text and self._first_h1 is None:
                    self._first_h1 = h1_text
                self._in_h1 = False
                self._h1_parts = []

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
        if self._in_html_title:
            self._html_title_parts.append(data)
        if self._in_page_title:
            self._page_title_parts.append(data)
        if self._in_h1:
            self._h1_parts.append(data)
        if self._in_heading:
            self._heading_parts.append(data)
        if self._in_content():
            self._content_text_parts.append(data)


_TABLE_RE = re.compile(r"<table\b[^>]*>.*?</table>", re.IGNORECASE | re.DOTALL)
_ROW_RE = re.compile(r"<tr\b[^>]*>.*?</tr>", re.IGNORECASE | re.DOTALL)
_TH_RE = re.compile(r"<th\b[^>]*>(.*?)</th>", re.IGNORECASE | re.DOTALL)
_TD_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.IGNORECASE | re.DOTALL)
_HREF_RE = re.compile(r"href\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


def _extract_abstract_publish_date(content_text: str) -> str | None:
    if not content_text:
        return None

    # Prefer the primary edition year when present, e.g. "2nd Edition (1984)".
    m = re.search(
        r"\b(?:\d+(?:st|nd|rd|th)\s+)?edition\s*\((\d{4})\)",
        content_text,
        flags=re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)}-01-01"

    report_title_match = re.search(
        r"Report\s*Title\s*:\s*(.+?)(?:Author\s*:|$)",
        content_text,
        flags=re.IGNORECASE,
    )
    if report_title_match:
        years = re.findall(r"\((\d{4})\)", report_title_match.group(1))
        if years:
            return f"{years[-1]}-01-01"

    return None


def _extract_issue_dates_by_pdf(
    html: str,
    *,
    current_url: str,
    content_element_id: str,
) -> dict[str, str]:
    by_pdf: dict[str, str] = {}

    content_match = re.search(
        rf'<[^>]+id=["\']{re.escape(content_element_id)}["\'][^>]*>(.*)</[^>]+>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    content_html = content_match.group(1) if content_match else html

    tables = _TABLE_RE.findall(content_html)
    for table_html in tables:
        if not re.search(
            r"<th\b[^>]*>\s*Issue\s*Date\s*</th>",
            table_html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            continue

        rows = _ROW_RE.findall(table_html)
        issue_idx: int | None = None
        for row_html in rows:
            header_cells = _TH_RE.findall(row_html)
            if header_cells and issue_idx is None:
                normalized_headers = [
                    clean_text(_TAG_RE.sub(" ", html_lib.unescape(c))).lower().rstrip(":")
                    for c in header_cells
                ]
                for idx, header in enumerate(normalized_headers):
                    if header == "issue date" or ("issue" in header and "date" in header):
                        issue_idx = idx
                        break
                continue

            if issue_idx is None:
                continue

            data_cells = _TD_RE.findall(row_html)
            if not data_cells or issue_idx >= len(data_cells):
                continue

            issue_text = clean_text(
                _TAG_RE.sub(" ", html_lib.unescape(data_cells[issue_idx]))
            )
            if not issue_text or issue_text == "-":
                continue

            hrefs = _HREF_RE.findall(row_html)
            for href in hrefs:
                abs_url = canonicalize_url(urljoin(current_url, href), encode_spaces=True)
                if not abs_url:
                    continue
                if path_ext(abs_url) != ".pdf":
                    continue
                if abs_url not in by_pdf:
                    by_pdf[abs_url] = issue_text

    return by_pdf


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
        cfg = ctx.get_crawler_config(self.name)

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

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen_pages: set[str] = set()
        seen_record_keys: set[tuple[str]] = set()

        seed_url = canonicalize_url(page_url, encode_spaces=True)
        leaflets_canonical = canonicalize_url(leaflets_url, encode_spaces=True)
        if not seed_url:
            return []

        seed_host = urlparse(seed_url).netloc.lower()
        queue: deque[_VisitItem] = deque(
            [_VisitItem(url=seed_url, discovered_from=seed_url, from_leaflets=False)]
        )

        def _append_record(
            *,
            record_url: str,
            title: str | None,
            discovered_from_url: str,
            publish_date: str | None = None,
        ) -> None:
            if len(out) >= max_total_records:
                return
            key = (record_url,)
            if key in seen_record_keys:
                return
            seen_record_keys.add(key)

            meta: dict[str, str | None] = {"discovered_from": discovered_from_url}

            out.append(
                ctx.make_record(
                    url=record_url,
                    name=title or None,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta=meta,
                    publish_date=publish_date,
                )
            )

        while queue and len(seen_pages) < max_pages and len(out) < max_total_records:
            item = queue.popleft()
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
            page_title = signals.preferred_page_name or infer_name_from_link(
                None, current_url
            )
            page_publish_date = _extract_abstract_publish_date(signals.content_text)
            issue_dates_by_pdf = _extract_issue_dates_by_pdf(
                html,
                current_url=current_url,
                content_element_id=content_element_id,
            )

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
                    pdf_publish_date = issue_dates_by_pdf.get(abs_url) or page_publish_date
                    _append_record(
                        record_url=abs_url,
                        title=title,
                        discovered_from_url=current_url,
                        publish_date=pdf_publish_date,
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
                    publish_date=page_publish_date,
                )
                continue

            if item.from_leaflets and direct_pdf_count == 0:
                _append_record(
                    record_url=current_url,
                    title=page_title,
                    discovered_from_url=item.discovered_from,
                    publish_date=page_publish_date,
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
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
