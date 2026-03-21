from __future__ import annotations

import logging
import random
from dataclasses import dataclass
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
from utils.html_links import extract_links

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_URL = (
    "https://www.epd.gov.hk/epd/english/resources_pub/publications/"
    "regional_cooperation_plan.html"
)


@dataclass(frozen=True)
class _PageLink:
    url: str
    text: str


@dataclass(frozen=True)
class _PdfLink:
    url: str
    text: str


class Crawler:
    name = "epd.publications_other_reports"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        http_cfg = ctx.get_http_config()

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))
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

        try:
            html = _fetch_html(
                session=session,
                url=page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base=backoff_base,
                backoff_jitter=backoff_jitter,
                request_delay=request_delay,
                request_jitter=request_jitter,
            )
        except Exception as exc:
            logger.warning("[%s] Failed to fetch page %s: %s", self.name, page_url, exc)
            return []

        page_links = _extract_page_links(html, base_url=page_url)
        pdf_links = _collect_pdf_links(page_links, base_url=page_url)

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()
        for link in pdf_links:
            if len(out) >= max_total_records:
                break
            if link.url in seen_urls:
                continue

            out.append(
                ctx.make_record(
                    url=link.url,
                    name=infer_name_from_link(link.text, link.url),
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    meta={"discovered_from": page_url},
                )
            )
            seen_urls.add(link.url)

        return out


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
    )
    resp.encoding = "utf-8"
    return resp.text or ""


def _sleep_with_jitter(base_delay: float, jitter: float) -> None:
    delay = max(0.0, float(base_delay))
    jitter_value = max(0.0, float(jitter))
    if jitter_value > 0:
        delay += random.uniform(0.0, jitter_value)
    if delay > 0:
        sleep_seconds(delay)


def _extract_page_links(html: str, *, base_url: str) -> list[_PageLink]:
    raw_links = extract_links(html, base_url=base_url)
    out: list[_PageLink] = []
    for link in raw_links:
        canonical = canonicalize_url(link.href, encode_spaces=True)
        if not canonical:
            continue

        parsed = urlparse(canonical)
        if parsed.scheme not in {"http", "https"}:
            continue

        out.append(_PageLink(url=canonical, text=clean_text(link.text)))
    return out


def _collect_pdf_links(links: list[_PageLink], *, base_url: str) -> list[_PdfLink]:
    out: list[_PdfLink] = []
    seen: set[str] = set()

    for link in links:
        pdf_url = _resolve_pdf_document_url(link.url, base_url=base_url)
        if not pdf_url or pdf_url in seen:
            continue
        seen.add(pdf_url)
        out.append(_PdfLink(url=pdf_url, text=link.text))

    return out


def _resolve_pdf_document_url(url: str, *, base_url: str) -> str | None:
    canonical = canonicalize_url(url, encode_spaces=True)
    if not canonical:
        return None

    if path_ext(canonical) == ".pdf":
        return canonical

    parsed = urlparse(canonical)
    if parsed.path.lower().endswith("/archive_pdf.html"):
        query = parse_qs(parsed.query)
        raw_pdf = clean_text((query.get("pdf") or [""])[0])
        if raw_pdf:
            resolved = canonicalize_url(urljoin(base_url, raw_pdf), encode_spaces=True)
            if resolved and path_ext(resolved) == ".pdf":
                return resolved

    return None
