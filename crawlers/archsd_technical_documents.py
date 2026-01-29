from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord
from utils.html_links import HtmlLink, extract_links, extract_links_in_element


_ALLOWED_DOC_EXTS = {".pdf"}


def _sleep_seconds(seconds: float) -> None:
    if seconds <= 0:
        return
    time.sleep(seconds)


def _compute_backoff_seconds(attempt: int, *, base: float, jitter: float) -> float:
    exp = base * (2**attempt)
    exp = min(exp, 30.0)
    if jitter > 0:
        exp += random.uniform(0.0, jitter)
    return exp


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout_seconds)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    resp.raise_for_status()

                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        _sleep_seconds(float(retry_after))
                    except ValueError:
                        pass

                _sleep_seconds(
                    _compute_backoff_seconds(
                        attempt,
                        base=backoff_base_seconds,
                        jitter=backoff_jitter_seconds,
                    )
                )
                continue

            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_err = e
            if attempt >= max_retries:
                raise

            _sleep_seconds(
                _compute_backoff_seconds(
                    attempt,
                    base=backoff_base_seconds,
                    jitter=backoff_jitter_seconds,
                )
            )

    assert last_err is not None
    raise last_err


def _canonicalize_url(url: str) -> str | None:
    s = (url or "").strip()
    if not s:
        return None

    lower = s.lower()
    if lower.startswith("javascript:"):
        return None
    if lower.startswith("mailto:"):
        return None
    if lower.startswith("tel:"):
        return None

    p = urlparse(s)
    if not p.scheme or not p.netloc:
        return None

    p = p._replace(scheme=p.scheme.lower(), netloc=p.netloc.lower(), fragment="")

    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    p = p._replace(path=path)

    return urlunparse(p)


def _path_ext(url: str) -> str:
    p = urlparse(url)
    path = (p.path or "").lower()
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


def _iter_links(html: str, *, base_url: str, content_element_id: str) -> Iterable[HtmlLink]:
    scoped = extract_links_in_element(
        html, base_url=base_url, element_id=content_element_id
    )
    if scoped:
        return scoped
    return extract_links(html, base_url)


_PDF_URL_RE = re.compile(
    r"((?:https?://|/)[^\"\'<>\s]+?\.pdf(?:\?[^\"\'<>\s]*)?)",
    re.IGNORECASE,
)


def _is_probable_year_link(link: HtmlLink) -> int | None:
    txt = (link.text or "").strip()
    if len(txt) != 4 or not txt.isdigit():
        return None
    year = int(txt)
    if year < 1900 or year > 2100:
        return None
    return year


_ITEM_WITH_DOCUMENTS_RE = re.compile(
    r"\{\s*title:\s*\"(?P<item_title>[^\"]*)\"\s*,\s*description:\s*\"(?P<desc>[^\"]*)\"\s*,\s*document:\s*\[(?P<docs>.*?)\]\s*\}",
    re.DOTALL,
)
_DOCUMENT_ENTRY_RE = re.compile(
    r"\{\s*title:\s*\"(?P<title>[^\"]*)\"\s*,\s*url:\s*\"(?P<url>[^\"]*)\"\s*,\s*icon:\s*\"(?P<icon>[^\"]*)\"\s*\}",
    re.DOTALL,
)


def _extract_pdf_titles_from_items_array(html: str, *, base_url: str) -> dict[str, str]:
    """Best-effort extraction of PDF (url -> title) from embedded JS datasets.

    ARCHSD Technical Documents pages often render listings client-side via a JS
    array (e.g. `var itemsArray = [...]`). In that case there may be few/no PDF
    anchor tags in the HTML, so we scrape the embedded dataset for titles.
    """

    out: dict[str, str] = {}
    if not html:
        return out

    for item_m in _ITEM_WITH_DOCUMENTS_RE.finditer(html):
        item_title = (item_m.group("item_title") or "").strip()
        docs = item_m.group("docs") or ""
        for doc_m in _DOCUMENT_ENTRY_RE.finditer(docs):
            icon = (doc_m.group("icon") or "").strip().lower()
            if icon != "pdf":
                continue

            doc_title = (doc_m.group("title") or "").strip()
            raw_url = (doc_m.group("url") or "").strip()
            if not raw_url:
                continue

            # The dataset occasionally contains literal spaces.
            raw_url = raw_url.replace(" ", "%20")
            raw_url = raw_url.replace("&amp;", "&")

            if raw_url.startswith("/"):
                raw_url = urljoin(base_url, raw_url)

            can = _canonicalize_url(raw_url)
            if not can:
                continue

            title = doc_title or item_title
            if not title:
                continue

            # Prefer the first non-empty title we see.
            if can not in out:
                out[can] = title
            elif not out[can] and title:
                out[can] = title

    return out


def _extract_pdf_urls_from_html(html: str, *, base_url: str) -> list[str]:
    out: list[str] = []
    for m in _PDF_URL_RE.findall(html or ""):
        s = (m or "").strip()
        if not s:
            continue
        # Common HTML entity.
        s = s.replace("&amp;", "&")

        # Trim trailing punctuation that sometimes gets captured.
        s = s.rstrip(")].,;\"'\u00bb\u2019\u201d")

        if s.startswith("/"):
            s = urljoin(base_url, s)
        out.append(s)

    # Preserve order while de-duping.
    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


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
    # Accept both with/without leading slash just in case.
    if not p.startswith("/") and ("/" + p) in allowed_paths:
        return True
    return False


@dataclass(frozen=True)
class _QueueItem:
    url: str
    depth: int
    discovered_from: str | None
    section: str | None
    section_root_slug: str | None


class Crawler:
    """ARCHSD Technical Documents crawler.

    Crawls the Technical Documents hub page, discovers the 6 section pages, then
    recursively follows subsection pages and emits PDF links.

        Year handling:
        - No year filtering. Keeps all PDFs regardless of year.

    Config: crawlers.archsd_technical_documents
      - base_url: https://www.archsd.gov.hk
      - start_url: https://www.archsd.gov.hk/en/reports/techinical-documents.html
      - min_year: 2022
      - content_element_id: content (best-effort)
      - allowed_page_path_prefixes:
          - /en/reports/
          - /reports/
          - /en/publications-publicity/
          - /publications-publicity/
      - max_depth: 6
      - max_pages: 2000
      - max_out_links_per_page: 800
      - request_delay_seconds: 0.25
      - request_jitter_seconds: 0.10
      - max_total_records: 50000
      - backoff_base_seconds: 0.5
      - backoff_jitter_seconds: 0.25
    """

    name = "archsd_technical_documents"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.archsd.gov.hk")).rstrip("/")
        start_url = str(
            cfg.get("start_url", f"{base_url}/en/reports/techinical-documents.html")
        ).strip()

        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        allowed_prefixes_raw = cfg.get("allowed_page_path_prefixes", None)
        allowed_page_path_prefixes: list[str]
        if isinstance(allowed_prefixes_raw, list):
            allowed_page_path_prefixes = [
                (str(v).strip() or "")
                for v in allowed_prefixes_raw
                if isinstance(v, str)
            ]
        else:
            allowed_page_path_prefixes = [
                "/en/publications-publicity/",
                "/publications-publicity/",
            ]

        max_depth = int(cfg.get("max_depth", 6))
        max_pages = int(cfg.get("max_pages", 2000))
        max_out_links_per_page = int(cfg.get("max_out_links_per_page", 800))

        allowed_pdf_prefixes_raw = cfg.get("allowed_pdf_path_prefixes", None)
        allowed_pdf_path_prefixes: list[str]
        if isinstance(allowed_pdf_prefixes_raw, list):
            allowed_pdf_path_prefixes = [
                (str(v).strip() or "")
                for v in allowed_pdf_prefixes_raw
                if isinstance(v, str)
            ]
        else:
            allowed_pdf_path_prefixes = ["/media/publications-publicity/"]

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.10))

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        base_parsed = urlparse(base_url)
        base_netloc = base_parsed.netloc.lower()

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        start_can = _canonicalize_url(start_url)
        if not start_can:
            return []

        ignored_page_paths: set[str] = {
            "/en/publications-publicity/standard-drawings.html",
        }

        # Always allow the hub URL(s) as crawlable HTML pages even though they sit
        # outside the publications-publicity path prefix.
        start_path = urlparse(start_can).path or "/"
        explicit_allowed_paths: set[str] = {
            start_path,
            "/en/reports/technical-documents.html",
            "/en/reports/techinical-documents.html",
            "/en/reports/technical-documents",
            "/en/reports/techinical-documents",
        }

        if ctx.debug:
            print(f"[{self.name}] start={start_can}")

        # 1) Fetch hub and discover section pages.
        resp = _get_with_retries(
            session,
            start_can,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )
        hub_links = list(
            _iter_links(resp.text, base_url=start_can, content_element_id=content_element_id)
        )

        section_slug_to_name = {
            "general-specifications.html": "General Specifications",
            "testing-commissioning-procedure.html": "Testing & Commissioning Procedure",
            "bill-of-quantities,-method-of-measurement-and-schedule-of-rates.html": "Model Bill of Quantities for Building Works",
            "standard-method-of-measurement-for-building-elements.html": "Standard Method of Measurement for Building Elements",
            "schedule-of-rates.html": "Schedule of Rates",
        }

        # Heuristic: many pages include global navigation links under the same
        # publications-publicity prefix (e.g., press releases). To avoid scope
        # creep, only recurse into subsection pages that match known slug prefixes
        # for the given section.
        section_child_slug_prefixes: dict[str, list[str]] = {
            "general-specifications.html": ["general-specification-for-"],
            "testing-commissioning-procedure.html": ["t-c-procedure-for-"],
        }

        expected_sections = {s.lower() for s in section_slug_to_name.values()}

        section_urls: list[tuple[str, str]] = []
        for link in hub_links:
            can = _canonicalize_url(link.href)
            if not can:
                continue
            p = urlparse(can)
            if p.netloc.lower() != base_netloc:
                continue
            if (p.path or "") in ignored_page_paths:
                continue
            slug = (p.path or "").rsplit("/", 1)[-1].strip().lower()
            if slug in section_slug_to_name:
                section_urls.append((section_slug_to_name[slug], can))
                continue

            text_norm = " ".join((link.text or "").split()).strip().lower()
            if text_norm in expected_sections:
                section_urls.append((link.text.strip() or text_norm, can))

        # Fallback: if text matching fails, crawl from start page as well.
        queue: list[_QueueItem] = []
        if section_urls:
            # Seed each section as its own root.
            for sec_name, sec_url in section_urls:
                sec_slug = (urlparse(sec_url).path or "").rsplit("/", 1)[-1].strip()
                queue.append(
                    _QueueItem(
                        url=sec_url,
                        depth=0,
                        discovered_from=start_can,
                        section=sec_name,
                        section_root_slug=(sec_slug.lower() or None),
                    )
                )
        else:
            queue.append(
                _QueueItem(
                    url=start_can,
                    depth=0,
                    discovered_from=None,
                    section=None,
                    section_root_slug=None,
                )
            )

        visited_pages: set[str] = set()
        skipped_pages: set[str] = set()
        seen_docs: set[str] = set()
        out: list[UrlRecord] = []

        while queue:
            item = queue.pop(0)

            if item.url in visited_pages or item.url in skipped_pages:
                continue
            if len(visited_pages) >= max_pages:
                break

            p = urlparse(item.url)
            if p.netloc.lower() != base_netloc:
                continue

            if (p.path or "") in ignored_page_paths:
                skipped_pages.add(item.url)
                continue

            # Only recurse through in-scope HTML pages.
            if not (
                _path_is_explicitly_allowed(p.path, allowed_paths=explicit_allowed_paths)
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

            links = list(
                _iter_links(resp.text, base_url=item.url, content_element_id=content_element_id)
            )
            if max_out_links_per_page > 0:
                links = links[:max_out_links_per_page]

            # Some ARCHSD pages embed PDF URLs in scripts or non-anchor attributes.
            # Extract them from raw HTML as a fallback.
            pdf_urls_in_html = _extract_pdf_urls_from_html(resp.text, base_url=item.url)

            # JS-rendered listings (e.g. itemsArray) contain PDF titles.
            pdf_title_map = _extract_pdf_titles_from_items_array(resp.text, base_url=item.url)

            # Emit PDF documents on the current page.
            doc_url_to_text: dict[str, str] = {}

            for link in links:
                can = _canonicalize_url(link.href)
                if not can:
                    continue
                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue
                doc_url_to_text[can] = link.text or ""

            for u in pdf_urls_in_html:
                can = _canonicalize_url(u)
                if not can:
                    continue
                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue
                if can not in doc_url_to_text:
                    doc_url_to_text[can] = ""

            # Fill missing names from embedded dataset.
            for doc_url, title in pdf_title_map.items():
                if doc_url in doc_url_to_text and not (doc_url_to_text[doc_url] or "").strip():
                    doc_url_to_text[doc_url] = title

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

                link_text = doc_url_to_text.get(can, "")

                seen_docs.add(can)
                ext = _path_ext(can)

                out.append(
                    UrlRecord(
                        url=can,
                        name=(link_text or None),
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "start_url": start_can,
                            "section": item.section,
                            "discovered_from": item.url,
                            "file_ext": ext.lstrip("."),
                        },
                    )
                )

                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

            # Recurse into linked pages.
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

                # Avoid global navigation within publications-publicity by limiting
                # recursion to expected subsection slug patterns.
                if item.section_root_slug:
                    child_slug = (lp.path or "").rsplit("/", 1)[-1].strip().lower()
                    allowed_prefixes = section_child_slug_prefixes.get(
                        item.section_root_slug
                    )

                    # Numeric year selector links should still be crawlable.
                    if _is_probable_year_link(link) is not None:
                        allowed_prefixes = None

                    # If we don't know this section's subsection patterns, stay
                    # on the section root page only (avoid global nav drift).
                    if not allowed_prefixes:
                        if child_slug != item.section_root_slug:
                            continue
                    else:
                        if not any(child_slug.startswith(pref) for pref in allowed_prefixes):
                            if child_slug != item.section_root_slug:
                                continue

                if (lp.path or "") in ignored_page_paths:
                    continue

                # Propagate section name best-effort.
                section = item.section
                if not section:
                    text_norm = (link.text or "").strip().lower()
                    if text_norm in expected_sections:
                        section = link.text.strip() or text_norm

                if can not in visited_pages:
                    queue.append(
                        _QueueItem(
                            url=can,
                            depth=item.depth + 1,
                            discovered_from=item.url,
                            section=section,
                            section_root_slug=item.section_root_slug,
                        )
                    )

        out.sort(key=lambda r: (r.url or ""))
        return out
