from __future__ import annotations

import logging
import random
from collections import deque
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import parse_qs, urljoin, urlparse

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

logger = logging.getLogger(__name__)

_DEFAULT_CONTENT_ELEMENT_ID = "content"
_DEFAULT_MAX_DEPTH = 6
_DEFAULT_MAX_PAGES = 1200
_DEFAULT_MAX_OUT_LINKS_PER_PAGE = 1200
_DEFAULT_ALLOWED_DOC_EXTENSIONS = {".pdf", ".doc", ".docx"}


@dataclass(frozen=True)
class _QueueItem:
    url: str
    discovered_from: str
    depth: int
    link_text: str


class _TitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_title = False
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._parts.append(data)

    def title(self) -> str:
        return clean_text("".join(self._parts))


class EnvironmentInHKBaseCrawler:
    name: str = ""

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        if not self.name:
            logger.warning("[%s] Crawler name not set", self.__class__.__name__)
            return []

        cfg = ctx.get_crawler_config(self.name)
        http_cfg = ctx.get_http_config()

        seed_url = _canonicalize(clean_text(str(cfg.get("seed_url") or "")))
        if not seed_url:
            logger.warning("[%s] seed_url not configured or invalid", self.name)
            return []

        en_scope = _ensure_scope_prefix(
            clean_text(str(cfg.get("english_scope_prefix") or ""))
        )
        tc_scope = _ensure_scope_prefix(
            clean_text(str(cfg.get("tc_scope_prefix") or ""))
        )
        allowed_prefixes = [prefix for prefix in [en_scope, tc_scope] if prefix]
        if not allowed_prefixes:
            logger.warning(
                "[%s] english_scope_prefix and/or tc_scope_prefix not configured",
                self.name,
            )
            return []

        content_element_id = clean_text(
            str(cfg.get("content_element_id") or _DEFAULT_CONTENT_ELEMENT_ID)
        )
        max_depth = max(0, int(cfg.get("max_depth", _DEFAULT_MAX_DEPTH)))
        max_pages = max(1, int(cfg.get("max_pages", _DEFAULT_MAX_PAGES)))
        max_out_links_per_page = max(
            1, int(cfg.get("max_out_links_per_page", _DEFAULT_MAX_OUT_LINKS_PER_PAGE))
        )

        allowed_doc_extensions = _normalize_allowed_extensions(
            cfg.get("allowed_document_extensions")
        )
        include_document_urls = _as_bool(cfg.get("include_document_urls", False))

        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = clean_text(str(http_cfg.get("user_agent", "")))
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.10))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen_record_urls: set[str] = set()
        queued_urls: set[str] = set()
        visited_pages: set[str] = set()

        queue: deque[_QueueItem] = deque()
        queue.append(
            _QueueItem(
                url=seed_url,
                discovered_from=seed_url,
                depth=0,
                link_text="",
            )
        )
        queued_urls.add(seed_url)

        tc_seed = _rewrite_english_to_tc(seed_url)
        if tc_seed and _is_in_scope(tc_seed, allowed_prefixes):
            queue.append(
                _QueueItem(
                    url=tc_seed,
                    discovered_from=seed_url,
                    depth=0,
                    link_text="",
                )
            )
            queued_urls.add(tc_seed)

        while queue and len(out) < max_total_records and len(visited_pages) < max_pages:
            item = queue.popleft()
            current_url = item.url
            if current_url in visited_pages:
                continue

            try:
                html = _fetch_html(
                    session=session,
                    url=current_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_jitter=backoff_jitter,
                    request_delay=request_delay,
                    request_jitter=request_jitter,
                )
            except Exception as exc:
                logger.warning(
                    "[%s] Failed to fetch page %s: %s", self.name, current_url, exc
                )
                continue

            visited_pages.add(current_url)

            page_title = _extract_page_title(html)
            page_name = (
                page_title
                or clean_text(item.link_text)
                or infer_name_from_link(item.link_text, current_url)
            )
            _append_record(
                out=out,
                seen_urls=seen_record_urls,
                ctx=ctx,
                source=self.name,
                url=current_url,
                name=page_name,
                discovered_from=item.discovered_from,
            )
            if len(out) >= max_total_records:
                break

            if item.depth >= max_depth:
                continue

            links = _extract_scoped_links(
                html=html,
                current_url=current_url,
                content_element_id=content_element_id,
            )
            if len(links) > max_out_links_per_page:
                links = links[:max_out_links_per_page]

            for link in links:
                candidate = _canonicalize(link.href)
                if not candidate:
                    continue

                if include_document_urls:
                    document_url = _resolve_document_url(
                        candidate,
                        base_url=current_url,
                        allowed_doc_extensions=allowed_doc_extensions,
                    )
                    if document_url and _is_allowed_document_url(document_url, seed_url):
                        if document_url in seen_record_urls:
                            continue
                        if not _url_exists(
                            session=session,
                            url=document_url,
                            timeout_seconds=timeout_seconds,
                            request_delay=request_delay,
                            request_jitter=request_jitter,
                        ):
                            continue
                        doc_name = clean_text(link.text) or infer_name_from_link(
                            link.text, document_url
                        )
                        _append_record(
                            out=out,
                            seen_urls=seen_record_urls,
                            ctx=ctx,
                            source=self.name,
                            url=document_url,
                            name=doc_name,
                            discovered_from=current_url,
                        )
                        if len(out) >= max_total_records:
                            break
                        continue

                if not _is_in_scope(candidate, allowed_prefixes):
                    continue

                if not _looks_like_page_url(candidate):
                    continue

                if candidate not in visited_pages and candidate not in queued_urls:
                    queue.append(
                        _QueueItem(
                            url=candidate,
                            discovered_from=current_url,
                            depth=item.depth + 1,
                            link_text=clean_text(link.text),
                        )
                    )
                    queued_urls.add(candidate)

                tc_variant = _rewrite_english_to_tc(candidate)
                if (
                    tc_variant
                    and _is_in_scope(tc_variant, allowed_prefixes)
                    and tc_variant not in visited_pages
                    and tc_variant not in queued_urls
                ):
                    queue.append(
                        _QueueItem(
                            url=tc_variant,
                            discovered_from=current_url,
                            depth=item.depth + 1,
                            link_text=clean_text(link.text),
                        )
                    )
                    queued_urls.add(tc_variant)

            if len(out) >= max_total_records:
                break

        logger.info(
            "[%s] Crawled %d pages, emitted %d records",
            self.name,
            len(visited_pages),
            len(out),
        )
        return out


def _normalize_allowed_extensions(raw: object) -> set[str]:
    if not isinstance(raw, list):
        return set(_DEFAULT_ALLOWED_DOC_EXTENSIONS)

    out: set[str] = set()
    for item in raw:
        ext = clean_text(str(item or "")).lower()
        if not ext:
            continue
        if not ext.startswith("."):
            ext = "." + ext
        out.add(ext)

    return out or set(_DEFAULT_ALLOWED_DOC_EXTENSIONS)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = clean_text(str(value)).lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", "", "none", "null"}:
        return False
    return bool(value)


def _extract_page_title(html: str) -> str:
    parser = _TitleParser()
    parser.feed(html)
    return parser.title()


def _sleep_with_jitter(base_delay: float, jitter: float) -> None:
    delay = max(0.0, float(base_delay))
    jitter_value = max(0.0, float(jitter))
    if jitter_value > 0:
        delay += random.uniform(0.0, jitter_value)
    if delay > 0:
        sleep_seconds(delay)


def _fetch_html(
    *,
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
) -> str:
    _sleep_with_jitter(request_delay, request_jitter)
    resp = get_with_retries(
        session,
        url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base,
        backoff_jitter_seconds=backoff_jitter,
        response_hook=_apply_charset_fix,
    )
    return resp.text or ""


def _apply_charset_fix(resp: requests.Response) -> None:
    content_type = (resp.headers.get("Content-Type") or "").lower()
    is_html = (
        ("text/html" in content_type)
        or ("application/xhtml" in content_type)
        or not content_type
    )
    if not is_html:
        return

    encoding = (resp.encoding or "").strip().lower()
    if encoding and encoding not in {"iso-8859-1", "latin-1"}:
        return

    guessed = (getattr(resp, "apparent_encoding", None) or "").strip()
    resp.encoding = guessed or "utf-8"


def _url_exists(
    *,
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    request_delay: float,
    request_jitter: float,
) -> bool:
    _sleep_with_jitter(request_delay, request_jitter)
    try:
        head = session.head(url, timeout=timeout_seconds, allow_redirects=True)
        if head.status_code == 200:
            return True
        if head.status_code != 405:
            return False
    except requests.RequestException:
        return False

    _sleep_with_jitter(request_delay, request_jitter)
    try:
        resp = session.get(
            url, timeout=timeout_seconds, allow_redirects=True, stream=True
        )
        try:
            return resp.status_code == 200
        finally:
            resp.close()
    except requests.RequestException:
        return False


def _extract_scoped_links(
    *, html: str, current_url: str, content_element_id: str
) -> list[HtmlLink]:
    if content_element_id:
        links = extract_links_in_element(
            html,
            base_url=current_url,
            element_id=content_element_id,
        )
        if links:
            return links
    return extract_links(html, base_url=current_url)


def _append_record(
    *,
    out: list[UrlRecord],
    seen_urls: set[str],
    ctx: RunContext,
    source: str,
    url: str,
    name: str | None,
    discovered_from: str,
) -> None:
    if url in seen_urls:
        return

    locale = _infer_locale_from_url(url) or _infer_locale_from_url(discovered_from)
    meta = {"discovered_from": discovered_from}
    if locale:
        meta["locale"] = locale

    out.append(
        ctx.make_record(
            url=url,
            name=name,
            discovered_at_utc=ctx.run_date_utc,
            source=source,
            meta=meta,
        )
    )
    seen_urls.add(url)


def _looks_like_page_url(url: str) -> bool:
    ext = path_ext(url)
    if not ext:
        return True
    return ext in {".html", ".htm", ".asp", ".aspx", ".php", ".jsp"}


def _resolve_document_url(
    url: str,
    *,
    base_url: str,
    allowed_doc_extensions: set[str],
) -> str | None:
    ext = path_ext(url)
    if ext in allowed_doc_extensions:
        return url

    parsed = urlparse(url)
    if parsed.path.lower().endswith("/archive_pdf.html"):
        query = parse_qs(parsed.query)
        raw_doc = clean_text((query.get("pdf") or [""])[0])
        if raw_doc:
            resolved = _canonicalize(urljoin(base_url, raw_doc))
            if resolved and path_ext(resolved) in allowed_doc_extensions:
                return resolved

    return None


def _is_allowed_document_url(url: str, seed_url: str) -> bool:
    seed_host = urlparse(seed_url).netloc.lower()
    doc_host = urlparse(url).netloc.lower()
    if not seed_host or not doc_host:
        return False
    return doc_host == seed_host


def _ensure_scope_prefix(prefix: str) -> str:
    canonical = _canonicalize(prefix)
    if not canonical:
        return ""
    return canonical if canonical.endswith("/") else canonical + "/"


def _is_in_scope(url: str, allowed_prefixes: list[str]) -> bool:
    return any(url.startswith(prefix) for prefix in allowed_prefixes)


def _rewrite_english_to_tc(url: str) -> str | None:
    token = "/epd/english/"
    if token not in url:
        return None
    return url.replace(token, "/epd/tc_chi/", 1)


def _infer_locale_from_url(url: str) -> str | None:
    lower = url.lower()
    if "/epd/english/" in lower:
        return "en"
    if "/epd/tc_chi/" in lower:
        return "tc"
    return None


def _canonicalize(url: str) -> str | None:
    canonical = canonicalize_url(url, encode_spaces=True)
    if not canonical:
        return None

    parsed = urlparse(canonical)
    if parsed.scheme not in {"http", "https"}:
        return None

    return canonical