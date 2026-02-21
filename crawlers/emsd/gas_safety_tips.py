from __future__ import annotations

import logging
import random
import requests
from urllib.parse import unquote, urlparse

from crawlers.base import (
    RunContext,
    UrlRecord,
    infer_name_from_link,
    path_ext,
    clean_text,
    get_with_retries,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element

logger = logging.getLogger(__name__)

_DEFAULT_START_URL = (
    "https://www.emsd.gov.hk/en/gas_safety/gas_safety_tips_to_users/index.html"
)
_DEFAULT_SCOPE_PREFIX = (
    "https://www.emsd.gov.hk/en/gas_safety/gas_safety_tips_to_users/"
)
_DEFAULT_CONTENT_ELEMENT_ID = "content"

_DEFAULT_FAQ_EN_URL = (
    "https://www.emsd.gov.hk/en/gas_safety/gas_safety_tips_to_users/"
    "frequently_asked_questions/index.html"
)
_DEFAULT_FAQ_TC_URL = (
    "https://www.emsd.gov.hk/tc/gas_safety/gas_safety_tips_to_users/"
    "frequently_asked_questions/index.html"
)
_DEFAULT_SUSPENSION_URL = (
    "https://www.emsd.gov.hk/en/gas_safety/gas_safety_tips_to_users/"
    "suspension_of_approval_of_gas_appliance_and_recall/index.html"
)


def _normalize_url(url: str) -> str:
    out = (url or "").strip()
    if "#" in out:
        out = out.split("#", 1)[0]
    return out


def _current_dir_prefix(url: str) -> str:
    path = urlparse(url).path
    if not path:
        return url.rstrip("/") + "/"
    if path.endswith("/"):
        return url
    return url.rsplit("/", 1)[0] + "/"


def _infer_page_name(url: str, link_text: str | None) -> str | None:
    text = clean_text(link_text)
    if text:
        return text

    p = urlparse(url)
    parts = [part for part in p.path.split("/") if part]
    if not parts:
        return None

    # Prefer the directory name when URL ends with /index.html
    if len(parts) >= 2 and parts[-1].lower() == "index.html":
        tail = parts[-2]
    else:
        tail = parts[-1]

    tail = unquote(tail)
    if "." in tail:
        tail = tail.rsplit(".", 1)[0]
    tail = tail.replace("_", " ").replace("-", " ")
    tail = clean_text(tail)
    return tail or None


class Crawler:
    name = "emsd.gas_safety_tips"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        crawler_cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        start_url = _normalize_url(
            str(crawler_cfg.get("start_url", _DEFAULT_START_URL)).strip()
        )
        scope_prefix = _normalize_url(
            str(crawler_cfg.get("scope_prefix", _DEFAULT_SCOPE_PREFIX)).strip()
        )
        content_element_id = str(
            crawler_cfg.get("content_element_id", _DEFAULT_CONTENT_ELEMENT_ID)
        ).strip()
        faq_en_url = _normalize_url(
            str(crawler_cfg.get("faq_en_url", _DEFAULT_FAQ_EN_URL)).strip()
        )
        faq_tc_url = _normalize_url(
            str(crawler_cfg.get("faq_tc_url", _DEFAULT_FAQ_TC_URL)).strip()
        )
        suspension_url = _normalize_url(
            str(crawler_cfg.get("suspension_url", _DEFAULT_SUSPENSION_URL)).strip()
        )

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(crawler_cfg.get("request_delay_seconds", 0.5))
        request_jitter = float(crawler_cfg.get("request_jitter_seconds", 0.25))
        max_total_records = int(crawler_cfg.get("max_total_records", 50000))
        backoff_base = float(crawler_cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(crawler_cfg.get("backoff_jitter_seconds", 0.25))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        records: list[UrlRecord] = []
        seen_record_urls: set[str] = set()

        # queue item: (page_url, discovered_from, keep_page_url_record, page_name)
        queue: list[tuple[str, str | None, bool, str | None]] = [
            (start_url, None, False, None)
        ]
        visited_pages: set[str] = set()

        while queue:
            current_url, discovered_from, keep_page_record, page_name_hint = queue.pop(0)
            current_url = _normalize_url(current_url)
            if not current_url:
                continue
            if current_url in visited_pages:
                continue
            visited_pages.add(current_url)

            if request_delay > 0:
                sleep_seconds(request_delay + random.uniform(0, request_jitter))

            logger.info(f"[{self.name}] Fetching page: {current_url}")
            try:
                resp = get_with_retries(
                    session,
                    current_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base,
                    backoff_jitter_seconds=backoff_jitter,
                )
                # EMSD pages are UTF-8; enforce to avoid mojibake in extracted text.
                resp.encoding = "utf-8"
            except Exception as exc:
                logger.error(f"[{self.name}] Failed to fetch {current_url}: {exc}")
                continue

            if keep_page_record and current_url not in seen_record_urls:
                page_name = _infer_page_name(current_url, page_name_hint)
                records.append(
                    UrlRecord(
                        url=current_url,
                        name=page_name,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta={"discovered_from": discovered_from or start_url},
                    )
                )
                seen_record_urls.add(current_url)

                if len(records) >= max_total_records:
                    logger.warning(
                        f"[{self.name}] Reached max_total_records={max_total_records}"
                    )
                    break

            links = extract_links_in_element(
                resp.text,
                base_url=current_url,
                element_id=content_element_id,
            )
            if not links:
                links = extract_links(resp.text, base_url=current_url)

            current_prefix = _current_dir_prefix(current_url)

            for link in links:
                candidate = _normalize_url(link.href)
                if not candidate:
                    continue

                ext = path_ext(candidate)
                is_pdf = ext == ".pdf"

                if is_pdf:
                    if candidate in seen_record_urls:
                        continue
                    pdf_name = clean_text(link.text) or infer_name_from_link(
                        link.text, candidate
                    )
                    records.append(
                        UrlRecord(
                            url=candidate,
                            name=pdf_name,
                            discovered_at_utc=ctx.run_date_utc,
                            source=self.name,
                            meta={"discovered_from": current_url},
                        )
                    )
                    seen_record_urls.add(candidate)

                    if len(records) >= max_total_records:
                        logger.warning(
                            f"[{self.name}] Reached max_total_records={max_total_records}"
                        )
                        break
                    continue

                # Crawl only English pages under configured root prefix.
                if not candidate.startswith(scope_prefix):
                    # Special exception: EN FAQ points to TC FAQ page; crawl TC FAQ for PDFs only.
                    if current_url == faq_en_url and candidate == faq_tc_url:
                        if candidate not in visited_pages:
                            queue.append((candidate, current_url, False, clean_text(link.text)))
                    continue

                # Ignore URLs that are not sub-pages of current page.
                if not candidate.startswith(current_prefix):
                    continue

                keep_candidate_page_record = True

                # Exception 1: do not keep EN FAQ page URL; crawl TC FAQ PDFs only.
                if candidate == faq_en_url:
                    keep_candidate_page_record = False
                    if faq_tc_url not in visited_pages:
                        queue.append((faq_tc_url, candidate, False, clean_text(link.text)))
                # Exception 2: crawl suspension page PDFs but do not keep page URL itself.
                elif candidate == suspension_url:
                    keep_candidate_page_record = False

                if candidate not in visited_pages and all(
                    queued[0] != candidate for queued in queue
                ):
                    queue.append(
                        (
                            candidate,
                            current_url,
                            keep_candidate_page_record,
                            clean_text(link.text),
                        )
                    )

            if len(records) >= max_total_records:
                break

        logger.info(
            f"[{self.name}] Crawled {len(visited_pages)} pages, emitted {len(records)} records"
        )

        return records
