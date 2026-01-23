from __future__ import annotations

import random
import re
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urljoin

import requests

from crawlers.base import RunContext, UrlRecord
from utils.html_links import extract_links_in_element


_DETAIL_HREF_RE = re.compile(r"^/gia/general/\d{6}/\d{2}/P\d+\.htm$", re.IGNORECASE)


def _parse_run_date(run_date_utc: str) -> date:
    # Expected format: YYYY-MM-DD (as used by main.py)
    return date.fromisoformat(run_date_utc)


def _sleep_seconds(seconds: float) -> None:
    if seconds <= 0:
        return
    time.sleep(seconds)


def _compute_backoff_seconds(attempt: int, *, base: float, jitter: float) -> float:
    # Exponential backoff with cap
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


class Crawler:
    """Crawl HKSAR Government Press Releases day-by-day.

    Strategy: iterate daily listing pages for the last N days and extract press
    release detail anchors contained within div#contentBody.

    Config: crawlers.hksar_press_releases
      - base_url: https://www.info.gov.hk
      - days_back: 730
      - listing_path_template: /gia/general/{yyyymm}/{dd}.htm
      - request_delay_seconds: 0.5
      - request_jitter_seconds: 0.25
      - per_day_limit: 200
      - max_total_records: 50000

    Uses http.timeout_seconds/user_agent/max_retries as shared settings.
    """

    name = "hksar_press_releases"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        base_url = str(cfg.get("base_url", "https://www.info.gov.hk")).rstrip("/")
        days_back = int(cfg.get("days_back", 730))
        listing_path_template = str(
            cfg.get("listing_path_template", "/gia/general/{yyyymm}/{dd}.htm")
        )

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        per_day_limit = int(cfg.get("per_day_limit", 200))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        end_date = _parse_run_date(ctx.run_date_utc)
        start_date = end_date - timedelta(days=days_back)

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        discovered_at = datetime.now(timezone.utc).isoformat()

        seen_urls: set[str] = set()
        out: list[UrlRecord] = []

        current = end_date
        while current >= start_date:
            yyyymm = current.strftime("%Y%m")
            dd = current.strftime("%d")
            listing_path = listing_path_template.format(yyyymm=yyyymm, dd=dd)
            listing_url = urljoin(f"{base_url}/", listing_path.lstrip("/"))

            if ctx.debug:
                print(f"[{self.name}] Fetch {current.isoformat()} -> {listing_url}")

            resp = _get_with_retries(
                session,
                listing_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )

            links = extract_links_in_element(
                resp.text,
                base_url=listing_url,
                element_id="contentBody",
            )

            day_count = 0
            for link in links:
                # Keep only press release detail anchors
                # - Most are absolute after normalization, but regex expects path
                href = link.href
                if not href:
                    continue

                # Accept normalized absolute URLs as long as their path matches
                # the canonical detail pattern.
                if href.startswith(base_url):
                    path = href[len(base_url) :]
                    if not path.startswith("/"):
                        path = "/" + path
                else:
                    # If extract_links_in_element normalized to a different host, ignore
                    continue

                if not _DETAIL_HREF_RE.match(path):
                    continue

                if href in seen_urls:
                    continue
                seen_urls.add(href)

                out.append(
                    UrlRecord(
                        url=href,
                        name=(link.text or None),
                        discovered_at_utc=discovered_at,
                        source=self.name,
                        meta={
                            "date_utc": current.isoformat(),
                            "listing_url": listing_url,
                        },
                    )
                )
                day_count += 1

                if day_count >= per_day_limit:
                    break
                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

            # Polite pacing between day pages
            delay = request_delay_seconds
            if request_jitter_seconds > 0:
                delay += random.uniform(0.0, request_jitter_seconds)
            _sleep_seconds(delay)

            current -= timedelta(days=1)

        # Deterministic ordering for stable diffs
        out.sort(key=lambda r: (r.url, r.meta.get("date_utc") or ""))
        return out
