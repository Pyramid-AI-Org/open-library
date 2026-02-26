from __future__ import annotations

from dataclasses import dataclass
import random
import time
from typing import Any, Protocol
from urllib.parse import unquote, urlparse, urlunparse

import requests


@dataclass(frozen=True)
class UrlRecord:
    url: str
    name: str | None
    discovered_at_utc: str  # ISO-8601 string
    source: str
    source_id: str  # Folder name (e.g., "devb", "bd")
    source_label: str  # Human-readable label (e.g., "The Development Bureau")
    meta: dict[str, Any]


@dataclass
class RunContext:
    run_date_utc: str
    started_at_utc: str
    settings: dict[str, Any]
    source_id: str  # Folder name for this crawler's source
    source_label: str  # Human-readable label for this source
    debug: bool = False

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
    
    def make_record(
        self,
        url: str,
        name: str | None,
        discovered_at_utc: str,
        source: str,
        meta: dict[str, Any] | None = None,
    ) -> UrlRecord:
        """
        Create a UrlRecord with source_id and source_label automatically populated.
        
        Args:
            url: The URL of the record
            name: Display name for the record
            discovered_at_utc: ISO-8601 timestamp when discovered
            source: The crawler name (e.g., "devb_press_releases")
            meta: Optional metadata dictionary
        
        Returns:
            UrlRecord with all fields populated
        """
        return UrlRecord(
            url=url,
            name=name,
            discovered_at_utc=discovered_at_utc,
            source=source,
            source_id=self.source_id,
            source_label=self.source_label,
            meta=meta or {},
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
