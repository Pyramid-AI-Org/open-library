from __future__ import annotations

from dataclasses import dataclass
import logging
import random
import re
from urllib.parse import parse_qs, urljoin, urlparse

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
from utils.html_links import extract_links

logger = logging.getLogger(__name__)

_AIR_START_URL = "https://www.epd.gov.hk/epd/english/environmentinhk/air/studyrpts/air_studyrpts.html"
_NOISE_START_URL = "https://www.epd.gov.hk/epd/english/environmentinhk/noise/studyrpts/noise_studyrpts.html"
_WASTE_START_URL = "https://www.epd.gov.hk/epd/english/environmentinhk/waste/studyrpts/waste_studyrpts.html"

_DEFAULT_START_URLS = [
    _AIR_START_URL,
    _NOISE_START_URL,
    _WASTE_START_URL,
]

_FERRIES_PAGE = "ferries_ULSD.html"
_PM25_PAGE = "pm25_study.html"
_ACID_RAIN_PAGE = "acid_rain_archives.html"

_YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
_ACID_TITLE_YEAR_RE = re.compile(
    r"Study\s+of\s+Acid\s+Rain\s+in\s+Hong\s+Kong\s*,\s*((?:19|20)\d{2})",
    flags=re.IGNORECASE,
)
_CHAPTER_RE = re.compile(r"\bchapter\s*[-_: ]*([0-9]{1,2})\b", flags=re.IGNORECASE)


@dataclass(frozen=True)
class _PageLink:
    url: str
    text: str


@dataclass(frozen=True)
class _PdfLink:
    url: str
    text: str


class Crawler:
    name = "epd.publications_study_report"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        http_cfg = ctx.get_http_config()

        start_urls = _normalize_start_urls(cfg.get("start_urls"))
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
        seen_urls: set[str] = set()

        for start_url in start_urls:
            if len(out) >= max_total_records:
                break

            try:
                html = _fetch_html(
                    session=session,
                    url=start_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_jitter=backoff_jitter,
                    request_delay=request_delay,
                    request_jitter=request_jitter,
                )
            except Exception as exc:
                logger.warning(
                    "[%s] Failed to fetch page %s: %s", self.name, start_url, exc
                )
                continue

            page_links = _extract_page_links(html, base_url=start_url)

            # Air page keeps legacy special handling for selected subpages.
            if _is_air_study_reports_page(start_url):
                _crawl_air_page(
                    out=out,
                    seen_urls=seen_urls,
                    ctx=ctx,
                    source=self.name,
                    start_url=start_url,
                    main_links=page_links,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_jitter=backoff_jitter,
                    request_delay=request_delay,
                    request_jitter=request_jitter,
                    max_total_records=max_total_records,
                    session=session,
                )
                continue

            # Noise/Waste: immediate PDFs only (no subpage traversal).
            for pdf_link in _collect_pdf_links(page_links, base_url=start_url):
                if len(out) >= max_total_records:
                    break
                _append_record(
                    out=out,
                    seen_urls=seen_urls,
                    ctx=ctx,
                    source=self.name,
                    link=pdf_link,
                    discovered_from=start_url,
                )

        return out


def _normalize_start_urls(raw_start_urls: object) -> list[str]:
    if not isinstance(raw_start_urls, list) or not raw_start_urls:
        return list(_DEFAULT_START_URLS)

    out: list[str] = []
    for item in raw_start_urls:
        url = clean_text(str(item))
        if url:
            out.append(url)

    # Keep deterministic processing order and avoid duplicate page crawls.
    seen: set[str] = set()
    deduped: list[str] = []
    for url in out or list(_DEFAULT_START_URLS):
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(url)

    return deduped


def _is_air_study_reports_page(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.path.lower().endswith("/air/studyrpts/air_studyrpts.html")


def _crawl_air_page(
    *,
    out: list[UrlRecord],
    seen_urls: set[str],
    ctx: RunContext,
    source: str,
    start_url: str,
    main_links: list[_PageLink],
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
    max_total_records: int,
    session: requests.Session,
) -> None:
    ferries_title = _default_study_title(_find_link_title(main_links, _FERRIES_PAGE))

    for pdf_link in _collect_pdf_links(main_links, base_url=start_url):
        if len(out) >= max_total_records:
            return
        _append_record(
            out=out,
            seen_urls=seen_urls,
            ctx=ctx,
            source=source,
            link=pdf_link,
            discovered_from=start_url,
        )

    if len(out) >= max_total_records:
        return

    subpages = _collect_allowed_subpages(main_links, base_url=start_url)

    ferries_url = subpages.get(_FERRIES_PAGE)
    if ferries_url and len(out) < max_total_records:
        _crawl_ferries_page(
            out=out,
            seen_urls=seen_urls,
            ctx=ctx,
            source=source,
            page_url=ferries_url,
            ferries_title=ferries_title,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_jitter=backoff_jitter,
            request_delay=request_delay,
            request_jitter=request_jitter,
            max_total_records=max_total_records,
            session=session,
        )

    pm25_url = subpages.get(_PM25_PAGE)
    if pm25_url and len(out) < max_total_records:
        _crawl_pm25_page(
            out=out,
            seen_urls=seen_urls,
            ctx=ctx,
            source=source,
            page_url=pm25_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_jitter=backoff_jitter,
            request_delay=request_delay,
            request_jitter=request_jitter,
            max_total_records=max_total_records,
            session=session,
        )

    acid_rain_url = subpages.get(_ACID_RAIN_PAGE)
    if acid_rain_url and len(out) < max_total_records:
        _crawl_acid_rain_page(
            out=out,
            seen_urls=seen_urls,
            ctx=ctx,
            source=source,
            page_url=acid_rain_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_jitter=backoff_jitter,
            request_delay=request_delay,
            request_jitter=request_jitter,
            max_total_records=max_total_records,
            session=session,
        )


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


def _collect_allowed_subpages(
    links: list[_PageLink], *, base_url: str
) -> dict[str, str]:
    out: dict[str, str] = {}
    for link in links:
        for slug in (_FERRIES_PAGE, _PM25_PAGE, _ACID_RAIN_PAGE):
            if link.url.lower().endswith(slug.lower()):
                out[slug] = (
                    canonicalize_url(urljoin(base_url, link.url), encode_spaces=True)
                    or link.url
                )
    return out


def _find_link_title(links: list[_PageLink], slug: str) -> str | None:
    for link in links:
        if link.url.lower().endswith(slug.lower()):
            text = clean_text(link.text)
            if text:
                return text
    return None


def _default_study_title(value: str | None) -> str:
    title = clean_text(value or "")
    if title:
        return title
    return "Trial of Local Ferries Using Ultra Low Sulphur Diesel"


def _append_record(
    *,
    out: list[UrlRecord],
    seen_urls: set[str],
    ctx: RunContext,
    source: str,
    link: _PdfLink,
    discovered_from: str,
    publish_year_fallback: int | None = None,
) -> None:
    if link.url in seen_urls:
        return

    name = infer_name_from_link(link.text, link.url)
    year = _extract_year(link.text)
    if year is None:
        year = publish_year_fallback
    publish_date = normalize_publish_date(year) if year is not None else None

    out.append(
        ctx.make_record(
            url=link.url,
            name=name,
            discovered_at_utc=ctx.run_date_utc,
            source=source,
            meta={"discovered_from": discovered_from},
            publish_date=publish_date,
        )
    )
    seen_urls.add(link.url)


def _crawl_ferries_page(
    *,
    out: list[UrlRecord],
    seen_urls: set[str],
    ctx: RunContext,
    source: str,
    page_url: str,
    ferries_title: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
    max_total_records: int,
    session: requests.Session,
) -> None:
    if len(out) >= max_total_records:
        return

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
        logger.warning(
            "[%s] Failed to fetch ferries page %s: %s", source, page_url, exc
        )
        return

    links = _extract_page_links(html, base_url=page_url)
    pdf_links = _collect_pdf_links(links, base_url=page_url)
    selected = _select_ferries_trial_report(pdf_links)
    if not selected:
        return

    base_title = clean_text(ferries_title)
    if not base_title:
        base_title = "Trial of Local Ferries Using Ultra Low Sulphur Diesel"

    if base_title.lower().endswith("trial report"):
        record_name = base_title
    else:
        record_name = f"{base_title} Trial Report"

    _append_record(
        out=out,
        seen_urls=seen_urls,
        ctx=ctx,
        source=source,
        link=_PdfLink(url=selected.url, text=record_name),
        discovered_from=page_url,
    )


def _select_ferries_trial_report(pdf_links: list[_PdfLink]) -> _PdfLink | None:
    best: _PdfLink | None = None
    best_score = -(10**9)

    for link in pdf_links:
        hay = f"{link.text} {link.url}".lower()
        score = 0
        if "trial report" in hay:
            score += 20
        if "trial" in hay:
            score += 10
        if "report" in hay:
            score += 3
        if "final report" in hay:
            score -= 6
        if "summary" in hay:
            score -= 2

        if score > best_score:
            best_score = score
            best = link

    if best is None or best_score <= 0:
        return None

    return best


def _crawl_pm25_page(
    *,
    out: list[UrlRecord],
    seen_urls: set[str],
    ctx: RunContext,
    source: str,
    page_url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
    max_total_records: int,
    session: requests.Session,
) -> None:
    if len(out) >= max_total_records:
        return

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
        logger.warning("[%s] Failed to fetch PM2.5 page %s: %s", source, page_url, exc)
        return

    links = _extract_page_links(html, base_url=page_url)
    for pdf_link in _collect_pdf_links(links, base_url=page_url):
        if len(out) >= max_total_records:
            return
        _append_record(
            out=out,
            seen_urls=seen_urls,
            ctx=ctx,
            source=source,
            link=pdf_link,
            discovered_from=page_url,
        )


def _crawl_acid_rain_page(
    *,
    out: list[UrlRecord],
    seen_urls: set[str],
    ctx: RunContext,
    source: str,
    page_url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
    max_total_records: int,
    session: requests.Session,
) -> None:
    if len(out) >= max_total_records:
        return

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
        logger.warning(
            "[%s] Failed to fetch acid rain page %s: %s", source, page_url, exc
        )
        return

    heading_year = _extract_acid_rain_heading_year(html)

    chapter_links: list[tuple[int, _PdfLink]] = []
    page_links = _extract_page_links(html, base_url=page_url)
    for pdf_link in _collect_pdf_links(page_links, base_url=page_url):
        chapter_num = _extract_chapter_num(pdf_link)
        if chapter_num is None:
            continue
        chapter_links.append((chapter_num, pdf_link))

    for chapter_num, pdf_link in sorted(chapter_links, key=lambda x: x[0]):
        if len(out) >= max_total_records:
            return
        _append_record(
            out=out,
            seen_urls=seen_urls,
            ctx=ctx,
            source=source,
            link=_PdfLink(
                url=pdf_link.url,
                text=f"Study of Acid Rain in Hong Kong Chapter {chapter_num}",
            ),
            discovered_from=page_url,
            publish_year_fallback=heading_year,
        )


def _extract_year(text: str) -> int | None:
    m = _YEAR_RE.search(clean_text(text))
    if not m:
        return None
    year = int(m.group(1))
    if 1900 <= year <= 2100:
        return year
    return None


def _extract_acid_rain_heading_year(html: str) -> int | None:
    m = _ACID_TITLE_YEAR_RE.search(html)
    if not m:
        return None
    year = int(m.group(1))
    if 1900 <= year <= 2100:
        return year
    return None


def _extract_chapter_num(link: _PdfLink) -> int | None:
    for candidate in (link.text, link.url):
        m = _CHAPTER_RE.search(candidate)
        if not m:
            continue
        num = int(m.group(1))
        if 1 <= num <= 99:
            return num
    return None
