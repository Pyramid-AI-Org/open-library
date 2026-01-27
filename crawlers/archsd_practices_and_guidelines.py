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

_PDF_URL_RE = re.compile(
    r"((?:https?://|/)[^\"\'<>\s]+?\.pdf(?:\?[^\"\'<>\s]*)?)",
    re.IGNORECASE,
)


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
    """ARCHSD Practices and Guidelines crawler.
    """

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
                str(v).strip() for v in allowed_pdf_path_prefixes_raw if isinstance(v, str)
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

        queue: list[_QueueItem] = [_QueueItem(url=start_can, depth=0, discovered_from=None)]

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

            pdf_urls_in_html = _extract_pdf_urls_from_html(resp.text, base_url=item.url)

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
