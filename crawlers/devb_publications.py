from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import unquote, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord
from crawlers.devb_standard_contract_documents import (
    STANDARD_CONTRACT_DOCS_PREFIX,
    parse_standard_contract_documents_page,
)
from crawlers.devb_standard_consultancy_documents import (
    STANDARD_CONSULTANCY_DOCS_PREFIX,
    parse_standard_consultancy_documents_page,
)
from utils.html_links import HtmlLink, extract_links, extract_links_in_element


_ALLOWED_DOC_EXTS = {".pdf", ".doc", ".docx"}


_GENERIC_LINK_TEXTS = {
    "more",
    "download",
    "click here",
    "here",
    "view",
    "open",
}


def _infer_doc_name(link_text: str | None, url: str) -> str | None:
    t = (link_text or "").strip()
    if t and t.lower() not in _GENERIC_LINK_TEXTS:
        return t

    p = urlparse(url)
    seg = (p.path or "").rstrip("/").rsplit("/", 1)[-1]
    if not seg:
        return t or None

    seg = unquote(seg)
    # Remove extension.
    if "." in seg:
        seg = seg.rsplit(".", 1)[0]

    # Normalize separators and whitespace.
    seg = seg.replace("_", " ").replace("-", " ")
    seg = " ".join(seg.split())

    if not seg:
        return t or None
    return seg


def _normalize_path_prefix(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return None
    if not s.startswith("/"):
        s = "/" + s
    # Keep trailing slash if provided; otherwise match exact or subtree.
    if len(s) > 1 and s.endswith("/"):
        s = s.rstrip("/") + "/"
    return s


def _path_is_excluded(path: str, *, excluded_prefixes: list[str]) -> bool:
    if not excluded_prefixes:
        return False
    p = path or "/"
    for pref in excluded_prefixes:
        if not pref:
            continue
        if pref.endswith("/"):
            if p.startswith(pref):
                return True
        else:
            if p == pref or p.startswith(pref + "/"):
                return True
    return False


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

    # Some DEVb links contain literal spaces in hrefs.
    # Encode them to keep output URLs usable.
    s = s.replace(" ", "%20")

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

    # Drop fragments; keep query (some pages use query flags).
    p = p._replace(scheme=p.scheme.lower(), netloc=p.netloc.lower(), fragment="")

    # Normalize path slightly.
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    p = p._replace(path=path)

    return urlunparse(p)


def _path_ext(url: str) -> str:
    p = urlparse(url)
    path = (p.path or "").lower()
    # Get last extension (handles .tar.gz poorly but we don't want zip/images anyway).
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


@dataclass(frozen=True)
class _QueueItem:
    url: str
    depth: int
    discovered_from: str | None


def _iter_links(
    html: str, *, base_url: str, content_element_id: str
) -> Iterable[HtmlLink]:
    # Prefer extracting from the main content container to avoid header/footer noise.
    scoped = extract_links_in_element(
        html, base_url=base_url, element_id=content_element_id
    )
    if scoped:
        return scoped
    return extract_links(html, base_url)


class Crawler:
    """Recursively crawl DevB Publications section and collect document links.

    Requirements:
    - Only emit document links (PDF/DOC/DOCX).
    - Only crawl HTML pages under `/en/publications_and_press_releases/publications/`.
    - Do not include images or zip files.

    Config: crawlers.devb_publications
      - base_url: https://www.devb.gov.hk
      - seed_url: publications index URL
      - scope_prefix: /en/publications_and_press_releases/publications/
      - content_element_id: content
      - max_depth: 6
      - max_pages: 200
      - max_out_links_per_page: 500
      - request_delay_seconds: 0.5
      - request_jitter_seconds: 0.25
      - max_total_records: 50000
      - backoff_base_seconds: 0.5
      - backoff_jitter_seconds: 0.25
    """

    name = "devb_publications"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.devb.gov.hk")).rstrip("/")
        seed_url = str(
            cfg.get(
                "seed_url",
                f"{base_url}/en/publications_and_press_releases/publications/index.html",
            )
        ).strip()

        scope_prefix = str(
            cfg.get("scope_prefix", "/en/publications_and_press_releases/publications/")
        ).strip()
        if not scope_prefix.startswith("/"):
            scope_prefix = "/" + scope_prefix

        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        excluded_prefixes_raw = cfg.get("exclude_path_prefixes", [])
        excluded_prefixes: list[str] = []
        if isinstance(excluded_prefixes_raw, list):
            for v in excluded_prefixes_raw:
                if not isinstance(v, str):
                    continue
                n = _normalize_path_prefix(v)
                if n:
                    excluded_prefixes.append(n)

        max_depth = int(cfg.get("max_depth", 6))
        max_pages = int(cfg.get("max_pages", 200))
        max_out_links_per_page = int(cfg.get("max_out_links_per_page", 500))

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))

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

        seed_can = _canonicalize_url(seed_url)
        if not seed_can:
            return []

        visited_pages: set[str] = set()
        skipped_pages: set[str] = set()
        seen_docs: set[str] = set()
        out: list[UrlRecord] = []

        queue: list[_QueueItem] = [
            _QueueItem(url=seed_can, depth=0, discovered_from=None)
        ]

        while queue:
            item = queue.pop(0)

            if item.url in visited_pages:
                continue
            if item.url in skipped_pages:
                continue
            if len(visited_pages) >= max_pages:
                break

            p = urlparse(item.url)
            if p.netloc.lower() != base_netloc:
                continue
            if _path_is_excluded(p.path, excluded_prefixes=excluded_prefixes):
                skipped_pages.add(item.url)
                continue
            if not p.path.startswith(scope_prefix):
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

            # Standard Consultancy Document pages also use complex tables where titles
            # must come from the "Document"/"Item" column, not from filename link text.
            # Delegate parsing to a focused helper.
            if p.path.startswith(STANDARD_CONSULTANCY_DOCS_PREFIX):
                doc_hits, page_links = parse_standard_consultancy_documents_page(
                    resp.text,
                    base_url=item.url,
                    content_element_id=content_element_id,
                )

                for hit in doc_hits:
                    can = _canonicalize_url(hit.url)
                    if not can:
                        continue

                    ext = _path_ext(can)
                    if ext not in _ALLOWED_DOC_EXTS:
                        continue

                    lp = urlparse(can)
                    if lp.netloc.lower() != base_netloc:
                        continue

                    if can in seen_docs:
                        continue
                    seen_docs.add(can)

                    meta: dict[str, object] = {
                        "seed_url": seed_can,
                        "discovered_from": item.url,
                        "depth": item.depth,
                        "file_ext": ext.lstrip("."),
                        "scope_page": item.url,
                        "standard_consultancy_document": True,
                    }
                    if hit.issue_date_raw:
                        meta["issue_date_raw"] = hit.issue_date_raw
                    if hit.meta:
                        meta.update(hit.meta)

                    out.append(
                        UrlRecord(
                            url=can,
                            name=hit.title,
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta=meta,
                        )
                    )

                    if len(out) >= max_total_records:
                        break

                if len(out) >= max_total_records:
                    break

                if item.depth < max_depth:
                    for next_url in page_links:
                        next_can = _canonicalize_url(next_url)
                        if not next_can:
                            continue

                        np = urlparse(next_can)
                        if np.netloc.lower() != base_netloc:
                            continue
                        if not np.path.startswith(STANDARD_CONSULTANCY_DOCS_PREFIX):
                            continue
                        if _path_is_excluded(np.path, excluded_prefixes=excluded_prefixes):
                            continue

                        if next_can not in visited_pages and next_can not in skipped_pages:
                            queue.append(
                                _QueueItem(
                                    url=next_can,
                                    depth=item.depth + 1,
                                    discovered_from=item.url,
                                )
                            )

                continue

            # DEVb Standard Contract Documents pages use complex tables (icon-only links,
            # clean/track columns, section header rows). Delegate parsing to a focused helper.
            if p.path.startswith(STANDARD_CONTRACT_DOCS_PREFIX):
                doc_hits, page_links = parse_standard_contract_documents_page(
                    resp.text,
                    base_url=item.url,
                    content_element_id=content_element_id,
                )

                for hit in doc_hits:
                    can = _canonicalize_url(hit.url)
                    if not can:
                        continue

                    ext = _path_ext(can)
                    if ext not in _ALLOWED_DOC_EXTS:
                        continue

                    lp = urlparse(can)
                    if lp.netloc.lower() != base_netloc:
                        continue

                    if can in seen_docs:
                        continue
                    seen_docs.add(can)

                    meta: dict[str, object] = {
                        "seed_url": seed_can,
                        "discovered_from": item.url,
                        "depth": item.depth,
                        "file_ext": ext.lstrip("."),
                        "scope_page": item.url,
                        "standard_contract_documents": True,
                    }
                    if hit.issue_date_raw:
                        meta["issue_date_raw"] = hit.issue_date_raw
                    if hit.meta:
                        meta.update(hit.meta)

                    out.append(
                        UrlRecord(
                            url=can,
                            name=hit.title,
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta=meta,
                        )
                    )

                    if len(out) >= max_total_records:
                        break

                if len(out) >= max_total_records:
                    break

                if item.depth < max_depth:
                    for next_url in page_links:
                        next_can = _canonicalize_url(next_url)
                        if not next_can:
                            continue

                        np = urlparse(next_can)
                        if np.netloc.lower() != base_netloc:
                            continue
                        if not np.path.startswith(STANDARD_CONTRACT_DOCS_PREFIX):
                            continue
                        if _path_is_excluded(np.path, excluded_prefixes=excluded_prefixes):
                            continue

                        if next_can not in visited_pages and next_can not in skipped_pages:
                            queue.append(
                                _QueueItem(
                                    url=next_can,
                                    depth=item.depth + 1,
                                    discovered_from=item.url,
                                )
                            )

                continue

            links = list(
                _iter_links(
                    resp.text, base_url=item.url, content_element_id=content_element_id
                )
            )
            if max_out_links_per_page > 0:
                links = links[:max_out_links_per_page]

            for link in links:
                can = _canonicalize_url(link.href)
                if not can:
                    continue

                ext = _path_ext(can)

                # Emit only documents.
                if ext in _ALLOWED_DOC_EXTS:
                    lp = urlparse(can)
                    if lp.netloc.lower() != base_netloc:
                        continue

                    if can in seen_docs:
                        continue
                    seen_docs.add(can)

                    out.append(
                        UrlRecord(
                            url=can,
                            name=_infer_doc_name(link.text, can),
                            discovered_at_utc=ctx.started_at_utc,
                            source=self.name,
                            meta={
                                "seed_url": seed_can,
                                "discovered_from": item.url,
                                "depth": item.depth,
                                "link_text": link.text,
                                "file_ext": ext.lstrip("."),
                                "scope_page": item.url,
                            },
                        )
                    )

                    if len(out) >= max_total_records:
                        break
                    continue

                # Recurse only into in-scope HTML pages.
                if item.depth >= max_depth:
                    continue

                lp = urlparse(can)
                if lp.netloc.lower() != base_netloc:
                    continue
                if not lp.path.startswith(scope_prefix):
                    continue
                if _path_is_excluded(lp.path, excluded_prefixes=excluded_prefixes):
                    continue

                if can not in visited_pages:
                    queue.append(
                        _QueueItem(
                            url=can, depth=item.depth + 1, discovered_from=item.url
                        )
                    )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        return out
