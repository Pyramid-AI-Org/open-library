from __future__ import annotations

import random
import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urljoin

import requests

from crawlers.base import RunContext, UrlRecord, get_with_retries, sleep_seconds
from utils.html_links import extract_links_in_element


_DETAIL_HREF_RE = re.compile(
    r"^/gia/general/\d{6}/\d{2}/P\d+(?:c)?\.htm$", re.IGNORECASE
)


def _parse_run_date(run_date_utc: str) -> date:
    # Expected format: YYYY-MM-DD (as used by main.py)
    return date.fromisoformat(run_date_utc)


_sleep_seconds = sleep_seconds


def _apply_charset_fix(resp: requests.Response) -> None:
    # Some pages may omit charset and `requests` can decode as ISO-8859-1,
    # which causes Chinese anchor text mojibake.
    content_type = (resp.headers.get("Content-Type") or "").lower()
    is_html = (
        ("text/html" in content_type)
        or ("application/xhtml" in content_type)
        or not content_type
    )
    if is_html:
        enc = (resp.encoding or "").strip().lower()
        if not enc or enc in ("iso-8859-1", "latin-1"):
            guessed = (getattr(resp, "apparent_encoding", None) or "").strip()
            resp.encoding = guessed or "utf-8"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> requests.Response:
    return get_with_retries(
        session,
        url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
        response_hook=_apply_charset_fix,
    )


class Crawler:
    """Crawl HKSAR Government Press Releases day-by-day.

    Strategy: iterate daily listing pages for the last N days and extract press
    release detail anchors contained within div#contentBody.

        Config: crawlers.hksar_press_releases
            - base_url: https://www.info.gov.hk
            - days_back: 730
            - locales: ["en", "tc"]
            - listing_path_template: /gia/general/{yyyymm}/{dd}.htm
            - listing_path_template_tc: /gia/general/{yyyymm}/{dd}c.htm
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

        locales_cfg = cfg.get("locales")
        if isinstance(locales_cfg, list) and locales_cfg:
            locales = [str(v).strip() for v in locales_cfg if str(v).strip()]
        else:
            locales = ["en"]

        listing_path_template_en = str(
            cfg.get("listing_path_template", "/gia/general/{yyyymm}/{dd}.htm")
        )
        listing_path_template_tc = str(
            cfg.get("listing_path_template_tc", "/gia/general/{yyyymm}/{dd}c.htm")
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

            for locale in locales:
                if locale == "en":
                    listing_path_template = listing_path_template_en
                elif locale == "tc":
                    listing_path_template = listing_path_template_tc
                else:
                    continue

                listing_path = listing_path_template.format(yyyymm=yyyymm, dd=dd)
                listing_url = urljoin(f"{base_url}/", listing_path.lstrip("/"))

                if ctx.debug:
                    print(
                        f"[{self.name}] Fetch {current.isoformat()} ({locale}) -> {listing_url}"
                    )

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
                    href = link.href
                    if not href:
                        continue

                    if href.startswith(base_url):
                        path = href[len(base_url) :]
                        if not path.startswith("/"):
                            path = "/" + path
                    else:
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
                                "locale": locale,
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

                delay = request_delay_seconds
                if request_jitter_seconds > 0:
                    delay += random.uniform(0.0, request_jitter_seconds)
                _sleep_seconds(delay)

            if len(out) >= max_total_records:
                break

            current -= timedelta(days=1)

        out.sort(key=lambda r: (r.url, r.meta.get("date_utc") or ""))
        return out
