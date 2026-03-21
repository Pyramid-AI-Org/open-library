from __future__ import annotations

import json
import logging
import random
import re
from html import unescape
from typing import Any
from urllib.parse import urljoin

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

logger = logging.getLogger(__name__)

_DEFAULT_LOCALES: list[dict[str, str]] = [
    {
        "locale": "english",
        "discovered_from": "https://www.epd.gov.hk/epd/english/resources_pub/publications/GN/GNindex.html",
        "api_url": "https://www.epd.gov.hk/epd/english/api/gn/gnbase.json",
    },
    {
        "locale": "tc_chi",
        "discovered_from": "https://www.epd.gov.hk/epd/tc_chi/resources_pub/publications/GN/GNindex.html",
        "api_url": "https://www.epd.gov.hk/epd/tc_chi/api/gn/gnbase.json",
    },
]

_DRUPAL_SETTINGS_RE = re.compile(
    r'<script type="application/json" data-drupal-selector="drupal-settings-json">(.*?)</script>',
    flags=re.IGNORECASE | re.DOTALL,
)


class Crawler:
    name = "epd.publications_guidance_documents"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)
        http_cfg = ctx.get_http_config()

        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        request_delay = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.1))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        locale_entries = _normalize_locales(cfg.get("locales"), _DEFAULT_LOCALES)

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for locale_cfg in locale_entries:
            if len(out) >= max_total_records:
                break

            locale = locale_cfg["locale"]
            locale_tag = _normalize_locale_tag(locale)
            discovered_from = locale_cfg["discovered_from"]
            api_url = locale_cfg["api_url"]

            programme_names: dict[str, str] = {}
            topic_names: dict[tuple[str, str], str] = {}
            try:
                listing_html = _fetch_text(
                    session=session,
                    url=discovered_from,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_jitter=backoff_jitter,
                    request_delay=request_delay,
                    request_jitter=request_jitter,
                )
                programme_names, topic_names = _extract_programme_topic_mappings(
                    listing_html
                )
            except Exception as exc:
                if ctx.debug:
                    logger.warning(
                        "[%s] Listing page unavailable for %s; continuing with API-only mode: %s",
                        self.name,
                        discovered_from,
                        exc,
                    )

            try:
                docs = _fetch_json(
                    session=session,
                    url=api_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_jitter=backoff_jitter,
                    request_delay=request_delay,
                    request_jitter=request_jitter,
                )
            except Exception as exc:
                logger.error("[%s] Failed to fetch API %s: %s", self.name, api_url, exc)
                continue

            if not isinstance(docs, list):
                if ctx.debug:
                    logger.warning(
                        "[%s] Unexpected GN API payload type at %s: %s",
                        self.name,
                        api_url,
                        type(docs).__name__,
                    )
                continue

            for item in docs:
                if len(out) >= max_total_records:
                    break
                if not isinstance(item, dict):
                    continue

                raw_href = clean_text(str(item.get("url", "")))
                if not raw_href:
                    continue

                canonical = canonicalize_url(
                    urljoin(discovered_from, raw_href), encode_spaces=True
                )
                if not canonical or path_ext(canonical) != ".pdf":
                    continue
                if canonical in seen_urls:
                    continue

                pid = clean_text(str(item.get("pid", "")))
                tid = clean_text(str(item.get("tid", "")))

                programme_area = programme_names.get(pid) or (pid or None)
                topic_area = topic_names.get((pid, tid)) or (tid or None)

                title = clean_text(str(item.get("title", ""))) or None
                code = clean_text(str(item.get("code", ""))) or None

                meta = {
                    "code": code,
                    "discovered_from": discovered_from,
                    "programme_area": programme_area,
                    "topic_area": topic_area,
                    "locale": locale_tag,
                }

                out.append(
                    ctx.make_record(
                        url=canonical,
                        name=title,
                        discovered_at_utc=ctx.run_date_utc,
                        source=self.name,
                        meta=meta,
                        publish_date=None,
                    )
                )
                seen_urls.add(canonical)

        return out


def _normalize_locales(
    raw_locales: Any, defaults: list[dict[str, str]]
) -> list[dict[str, str]]:
    if not isinstance(raw_locales, list) or not raw_locales:
        return [dict(x) for x in defaults]

    out: list[dict[str, str]] = []
    for item in raw_locales:
        if not isinstance(item, dict):
            continue

        locale = clean_text(str(item.get("locale", "")))
        discovered_from = clean_text(str(item.get("discovered_from", "")))
        api_url = clean_text(str(item.get("api_url", "")))

        if not locale or not discovered_from or not api_url:
            continue

        out.append(
            {
                "locale": locale,
                "discovered_from": discovered_from,
                "api_url": api_url,
            }
        )

    if out:
        return out

    return [dict(x) for x in defaults]


def _normalize_locale_tag(value: str) -> str:
    v = clean_text(value).lower()
    if v in {"en", "english"}:
        return "en"
    if v in {"tc", "tc_chi", "zh-hant", "zh_hant", "traditional_chinese"}:
        return "tc"
    return v or "en"


def _fetch_text(
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


def _fetch_json(
    *,
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_base: float,
    backoff_jitter: float,
    request_delay: float,
    request_jitter: float,
) -> Any:
    raw = _fetch_text(
        session=session,
        url=url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base=backoff_base,
        backoff_jitter=backoff_jitter,
        request_delay=request_delay,
        request_jitter=request_jitter,
    )
    return json.loads(raw)


def _sleep_with_jitter(delay: float, jitter: float) -> None:
    if delay <= 0:
        return
    sleep_seconds(delay + random.uniform(0.0, max(0.0, jitter)))


def _extract_programme_topic_mappings(
    listing_html: str,
) -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    programme_names: dict[str, str] = {}
    topic_names: dict[tuple[str, str], str] = {}

    m = _DRUPAL_SETTINGS_RE.search(listing_html or "")
    if not m:
        return programme_names, topic_names

    raw_json = unescape(m.group(1)).strip()
    if not raw_json:
        return programme_names, topic_names

    try:
        drupal_settings = json.loads(raw_json)
    except json.JSONDecodeError:
        return programme_names, topic_names

    gn = drupal_settings.get("gninitModule", {}).get("gn", {})
    if not isinstance(gn, dict):
        return programme_names, topic_names

    for pid_raw, prog_obj in gn.items():
        pid = clean_text(str(pid_raw))
        if not pid or not isinstance(prog_obj, dict):
            continue

        prog_name = clean_text(str(prog_obj.get("name", "")))
        if prog_name:
            programme_names[pid] = prog_name

        topics = prog_obj.get("topics", {})
        if not isinstance(topics, dict):
            continue

        for tid_raw, topic_obj in topics.items():
            tid = clean_text(str(tid_raw))
            if not tid or not isinstance(topic_obj, dict):
                continue
            topic_name = clean_text(str(topic_obj.get("name", "")))
            if topic_name:
                topic_names[(pid, tid)] = topic_name

    return programme_names, topic_names
