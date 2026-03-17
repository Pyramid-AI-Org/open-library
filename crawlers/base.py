from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import random
import re
import time
from typing import Any, Protocol
from urllib.parse import unquote, urlparse, urlunparse

import requests


@dataclass(frozen=True)
class UrlRecord:
    url: str
    name: str | None
    discovered_at_utc: str  # ISO-8601 string
    publish_date: str | None  # ISO date (YYYY-MM-DD) or null
    source: str
    source_id: str  # Folder name (e.g., "devb", "bd")
    source_label: str  # Human-readable label (e.g., "The Development Bureau")
    meta: dict[str, Any]


_UNSET = object()
_PUBLISH_DATE_META_KEYS: tuple[str, ...] = (
    "publish_date",
    "date_utc",
    "date",
    "publication_date",
    "published_date",
    "date_of_issue",
    "issue_date",
    "issue_date_raw",
    "edition_date",
    "year",
)


def _parse_with_formats(value: str, formats: tuple[str, ...]) -> str | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def normalize_publish_date(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, int) and 1000 <= value <= 9999:
        return f"{value:04d}-01-01"

    if isinstance(value, str):
        text = clean_text(value)
        if not text:
            return None

        parse_text = re.sub(r"\bsept\b", "Sep", text, flags=re.IGNORECASE)

        if re.fullmatch(r"\d{4}", parse_text):
            return f"{parse_text}-01-01"

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", parse_text):
            return parse_text

        iso_text = parse_text
        if iso_text.endswith("Z"):
            iso_text = f"{iso_text[:-1]}+00:00"
        try:
            return datetime.fromisoformat(iso_text).date().isoformat()
        except ValueError:
            pass

        parsed = _parse_with_formats(
            parse_text,
            (
                "%Y/%m/%d",
                "%d.%m.%Y",
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%d %b %Y",
                "%d %B %Y",
                "%b %d, %Y",
                "%B %d, %Y",
                "%b %Y",
                "%B %Y",
                "%Y %b",
                "%Y %B",
            ),
        )
        if parsed:
            return parsed

        for pattern, fmt in (
            (r"\b\d{4}-\d{2}-\d{2}\b", "%Y-%m-%d"),
            (r"\b\d{4}/\d{2}/\d{2}\b", "%Y/%m/%d"),
            (r"\b\d{2}\.\d{2}\.\d{4}\b", "%d.%m.%Y"),
            (r"\b\d{2}/\d{2}/\d{4}\b", "%d/%m/%Y"),
            (r"\b\d{2}-\d{2}-\d{4}\b", "%d-%m-%Y"),
            (
                r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{4}\b",
                "%b %Y",
            ),
            (
                r"\b\d{4}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\b",
                "%Y %b",
            ),
            (r"\b\d{4}\b", "%Y"),
        ):
            m = re.search(pattern, parse_text)
            if not m:
                continue
            token = m.group(0)
            if fmt == "%b %Y":
                parsed = _parse_with_formats(token, ("%b %Y", "%B %Y"))
            elif fmt == "%Y %b":
                parsed = _parse_with_formats(token, ("%Y %b", "%Y %B"))
            else:
                parsed = _parse_with_formats(token, (fmt,))
            if parsed:
                if fmt == "%Y":
                    return f"{token}-01-01"
                return parsed

    return None


def infer_publish_date_from_meta(meta: dict[str, Any] | None) -> str | None:
    if not meta:
        return None

    for key in _PUBLISH_DATE_META_KEYS:
        if key not in meta:
            continue
        normalized = normalize_publish_date(meta.get(key))
        if normalized:
            return normalized

    return None


@dataclass
class RunContext:
    run_date_utc: str
    started_at_utc: str
    settings: dict[str, Any]
    source_id: str  # Folder name for this crawler's source
    source_label: str  # Human-readable label for this source
    debug: bool = False
    prior_records_by_url: dict[str, dict[str, Any]] | None = None

    def get_crawler_config(self, crawler_name: str) -> dict[str, Any]:
        """
        Get configuration for a specific crawler, merging source-level and page-level settings.

        New settings structure:
          crawlers:
            devb:
              label: "The Development Bureau"
              base_url: "https://www.devb.gov.hk"
              pages:
                devb_press_releases:
                  years_back: 10
                  ...

        Page-level settings override source-level settings.
        """
        crawlers_cfg = self.settings.get("crawlers", {})
        source_cfg = crawlers_cfg.get(self.source_id, {})
        pages_cfg = source_cfg.get("pages", {})

        page_cfg = pages_cfg.get(crawler_name, {})
        if not page_cfg and "." in crawler_name:
            page_cfg = pages_cfg.get(crawler_name.rsplit(".", 1)[-1], {})

        # Merge: source-level defaults + page-level overrides
        merged = {}
        for k, v in source_cfg.items():
            if k != "pages" and k != "label":
                merged[k] = v
        merged.update(page_cfg)
        return merged

    def get_http_config(self) -> dict[str, Any]:
        """Get HTTP configuration from settings."""
        return self.settings.get("http", {})

    def get_prior_record(self, url: str) -> dict[str, Any] | None:
        """Get prior-run record by URL for the current crawler/source context."""
        if not self.prior_records_by_url:
            return None
        key = (url or "").strip()
        if not key:
            return None
        rec = self.prior_records_by_url.get(key)
        return rec if isinstance(rec, dict) else None

    def make_record(
        self,
        url: str,
        name: str | None,
        discovered_at_utc: str,
        source: str,
        meta: dict[str, Any] | None = None,
        publish_date: str | None | object = _UNSET,
    ) -> UrlRecord:
        """
        Create a UrlRecord with source_id and source_label automatically populated.

        Args:
            url: The URL of the record
            name: Display name for the record
            discovered_at_utc: ISO-8601 timestamp when discovered
            source: The crawler name (e.g., "devb_press_releases")
            meta: Optional metadata dictionary
            publish_date: Optional normalized publish date (YYYY-MM-DD)

        Returns:
            UrlRecord with all fields populated
        """
        meta_data = meta or {}
        resolved_publish_date = (
            infer_publish_date_from_meta(meta_data)
            if publish_date is _UNSET
            else normalize_publish_date(publish_date)
        )

        return UrlRecord(
            url=url,
            name=name,
            discovered_at_utc=discovered_at_utc,
            publish_date=resolved_publish_date,
            source=source,
            source_id=self.source_id,
            source_label=self.source_label,
            meta=meta_data,
        )


class BaseCrawler(Protocol):
    name: str

    def crawl(self, ctx: RunContext) -> list[UrlRecord]: ...


def clean_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def sleep_seconds(seconds: float) -> None:
    if seconds <= 0:
        return
    time.sleep(seconds)


def compute_backoff_seconds(
    attempt: int,
    *,
    base: float,
    jitter: float,
    max_backoff_seconds: float = 30.0,
) -> float:
    exp = base * (2**attempt)
    exp = min(exp, max_backoff_seconds)
    if jitter > 0:
        exp += random.uniform(0.0, jitter)
    return exp


def get_with_retries(
    session,
    url,
    *,
    timeout_seconds,
    max_retries,
    backoff_base_seconds,
    backoff_jitter_seconds,
    params=None,
    retry_statuses=(429, 500, 502, 503, 504),
    parse_retry_after_seconds=True,
    response_hook=None,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout_seconds)
            if resp.status_code in retry_statuses:
                if attempt >= max_retries:
                    resp.raise_for_status()

                if parse_retry_after_seconds:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            sleep_seconds(float(retry_after))
                        except ValueError:
                            pass

                sleep_seconds(
                    compute_backoff_seconds(
                        attempt,
                        base=backoff_base_seconds,
                        jitter=backoff_jitter_seconds,
                    )
                )
                continue

            resp.raise_for_status()
            if response_hook is not None:
                response_hook(resp)
            return resp
        except requests.RequestException as e:
            last_err = e
            if attempt >= max_retries:
                raise

            sleep_seconds(
                compute_backoff_seconds(
                    attempt,
                    base=backoff_base_seconds,
                    jitter=backoff_jitter_seconds,
                )
            )

    assert last_err is not None
    raise last_err


def canonicalize_url(
    url: str,
    *,
    reject_schemes=("javascript", "mailto", "tel"),
    encode_spaces=False,
    lowercase_scheme_host=True,
    strip_fragment=True,
    trim_trailing_slash=True,
    allowed_host: str | None = None,
) -> str | None:
    s = (url or "").strip()
    if not s:
        return None

    if encode_spaces:
        s = s.replace(" ", "%20")

    lower = s.lower()
    for sch in reject_schemes:
        if lower.startswith(f"{sch.lower()}:"):
            return None

    p = urlparse(s)
    if not p.scheme or not p.netloc:
        return None

    scheme = p.scheme.lower() if lowercase_scheme_host else p.scheme
    netloc = p.netloc.lower() if lowercase_scheme_host else p.netloc

    if allowed_host is not None and netloc != allowed_host.lower():
        return None

    fragment = "" if strip_fragment else p.fragment
    path = p.path or "/"
    if trim_trailing_slash and path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    p = p._replace(
        scheme=scheme,
        netloc=netloc,
        fragment=fragment,
        path=path,
    )
    return urlunparse(p)


def path_ext(url: str) -> str:
    p = urlparse(url)
    path = (p.path or "").lower()
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


def infer_name_from_link(link_text: str | None, url: str) -> str | None:
    text = clean_text(link_text)
    if text:
        return text

    p = urlparse(url)
    tail = (p.path or "").rstrip("/").rsplit("/", 1)[-1]
    if not tail:
        return None
    tail = unquote(tail)
    if "." in tail:
        tail = tail.rsplit(".", 1)[0]
    tail = tail.replace("_", " ").replace("-", " ")
    tail = clean_text(tail)
    return tail or None
