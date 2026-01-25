from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import RunContext, UrlRecord


_DETAIL_PATH_RE = re.compile(
    r"^/en/publications_and_press_releases/press/index_id_\d+\.html$", re.IGNORECASE
)


def _infer_max_pages_from_html(html: str, *, year: int, type_value: str) -> int | None:
    # Some DevB pages embed page index links in `divHiddenLinks` instead of (or in
    # addition to) a visible pagination widget.
    # Example: index_year_2025-type_all-page_11.html
    pat = re.compile(
        rf"index_year_{year}-type_{re.escape(type_value)}-page_(\d+)\.html",
        re.IGNORECASE,
    )

    max_page: int | None = None
    for m in pat.finditer(html or ""):
        try:
            n = int(m.group(1))
        except ValueError:
            continue
        if n <= 0:
            continue
        if max_page is None or n > max_page:
            max_page = n
    return max_page


def _parse_run_year(run_date_utc: str) -> int:
    # Expected format: YYYY-MM-DD
    return date.fromisoformat(run_date_utc).year


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


def _parse_ddmmyyyy_to_iso(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None


@dataclass
class _ListingRow:
    date_iso: str | None
    title: str | None
    href: str | None


class _DevbListingParser(HTMLParser):
    """Parses DevB press release listing pages.

    Extracts rows from `table.articlelistpage` where each row contains:
    - a date cell (dd/mm/yyyy)
    - a subject link (href + title)

    Also captures max page count from:
    - `<input id="pageGoInput" max="N">`
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_ListingRow] = []
        self.max_pages: int | None = None

        self._in_pagination = False
        self._pagination_depth = 0
        self._pagination_pages: set[int] = set()

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._tr_depth = 0
        self._current_date_parts: list[str] = []
        self._current_href: str | None = None
        self._current_title_parts: list[str] = []

        self._capture_date = False
        self._capture_title = False

        self._in_th = False
        self._th_depth = 0

    def _attrs_to_dict(self, attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "div":
            cls = attrs_map.get("class", "")
            if not self._in_pagination and "pagination" in cls.split():
                self._in_pagination = True
                self._pagination_depth = 1
                self._pagination_pages = set()
        elif self._in_pagination:
            self._pagination_depth += 1

        if self._in_pagination and t == "a":
            dp = attrs_map.get("data-page")
            if dp:
                try:
                    self._pagination_pages.add(int(dp))
                except ValueError:
                    pass

        if t == "input" and (attrs_map.get("id", "").lower() == "pagegoinput"):
            max_attr = attrs_map.get("max")
            if max_attr:
                try:
                    self.max_pages = int(max_attr)
                except ValueError:
                    pass

        if not self._in_table and t == "table":
            cls = attrs_map.get("class", "")
            if "articlelistpage" in cls.split():
                self._in_table = True
                self._table_depth = 1
                return

        if self._in_table:
            self._table_depth += 1

        if not self._in_table:
            return

        if t == "tr":
            self._in_tr = True
            self._tr_depth = 1
            self._current_date_parts = []
            self._current_href = None
            self._current_title_parts = []
            self._capture_date = False
            self._capture_title = False
            return

        if self._in_tr:
            self._tr_depth += 1

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            self._th_depth = 1
            return

        if self._in_th:
            self._th_depth += 1
            return

        if t == "td":
            cls = attrs_map.get("class", "")
            if "normalletterspacing" in cls and "t_center" in cls:
                self._capture_date = True
            return

        if t == "a" and self._current_href is None:
            href = attrs_map.get("href")
            if href:
                self._current_href = href
                self._capture_title = True
            return

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if self._in_pagination:
            self._pagination_depth -= 1
            if self._pagination_depth == 0:
                self._in_pagination = False
                if self._pagination_pages:
                    inferred = max(self._pagination_pages)
                    if not self.max_pages or inferred > self.max_pages:
                        self.max_pages = inferred

        if self._in_table:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
                self._in_tr = False
                self._capture_date = False
                self._capture_title = False
                return

        if not self._in_table:
            return

        if self._in_th:
            self._th_depth -= 1
            if self._th_depth == 0:
                self._in_th = False
            return

        if self._in_tr:
            self._tr_depth -= 1
            if self._tr_depth == 0 and t == "tr":
                self._in_tr = False

                if self._current_href:
                    title = "".join(self._current_title_parts).strip() or None
                    date_raw = "".join(self._current_date_parts).strip()
                    date_iso = _parse_ddmmyyyy_to_iso(date_raw)

                    self.rows.append(
                        _ListingRow(
                            date_iso=date_iso,
                            title=title,
                            href=self._current_href,
                        )
                    )

                self._capture_date = False
                self._capture_title = False
                return

        if self._capture_title and t == "a":
            self._capture_title = False
            return

        if self._capture_date and t == "td":
            self._capture_date = False
            return

    def handle_data(self, data: str) -> None:
        if not self._in_table or not self._in_tr or self._in_th:
            return

        if self._capture_date:
            self._current_date_parts.append(data)
        elif self._capture_title:
            self._current_title_parts.append(data)


class Crawler:
    """Crawl DevB Press Releases (English).

    Listing pages:
      https://www.devb.gov.hk/en/publications_and_press_releases/press/index_year_{yyyy}-type_all-page_{idx}.html

    Extracts press release detail URLs plus title and listing date.

    Config: crawlers.devb_press_releases
      - base_url: https://www.devb.gov.hk
      - years_back: 10
      - type: all
      - request_delay_seconds: 0.5
      - request_jitter_seconds: 0.25
      - max_total_records: 50000
      - backoff_base_seconds: 0.5
      - backoff_jitter_seconds: 0.25

    Uses http.timeout_seconds/user_agent/max_retries as shared settings.
    """

    name = "devb_press_releases"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.devb.gov.hk")).rstrip("/")
        years_back = int(cfg.get("years_back", 10))
        type_value = str(cfg.get("type", "all")).strip() or "all"

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        end_year = _parse_run_year(ctx.run_date_utc)
        start_year = max(1999, end_year - max(1, years_back) + 1)

        discovered_at = ctx.started_at_utc

        seen_urls: set[str] = set()
        out: list[UrlRecord] = []

        for year in range(end_year, start_year - 1, -1):
            # Fetch first page to discover max pages.
            first_url = (
                f"{base_url}/en/publications_and_press_releases/press/"
                f"index_year_{year}-type_{type_value}-page_1.html"
            )

            if ctx.debug:
                print(f"[{self.name}] Fetch {year} p1 -> {first_url}")

            try:
                first_resp = _get_with_retries(
                    session,
                    first_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except requests.HTTPError as e:
                # If the year doesn't exist yet (future) or is missing, skip.
                if getattr(e.response, "status_code", None) in (404,):
                    continue
                raise

            parser = _DevbListingParser()
            parser.feed(first_resp.text)
            inferred_max = _infer_max_pages_from_html(
                first_resp.text, year=year, type_value=type_value
            )
            max_pages = parser.max_pages or inferred_max or 1

            def _consume_rows(listing_url: str, rows: list[_ListingRow]) -> int:
                added = 0
                for row in rows:
                    if not row.href:
                        continue
                    abs_url = urljoin(listing_url, row.href)

                    # English only + expected detail path.
                    if not abs_url.startswith(base_url + "/en/"):
                        continue

                    path = abs_url[len(base_url) :]
                    if not _DETAIL_PATH_RE.match(path):
                        continue

                    if abs_url in seen_urls:
                        continue
                    seen_urls.add(abs_url)

                    out.append(
                        UrlRecord(
                            url=abs_url,
                            name=row.title,
                            discovered_at_utc=discovered_at,
                            source=self.name,
                            meta={
                                "date": row.date_iso,
                                "year": year,
                                "type": type_value,
                                "listing_url": listing_url,
                            },
                        )
                    )
                    added += 1

                    if len(out) >= max_total_records:
                        break
                return added

            _consume_rows(first_url, parser.rows)
            if len(out) >= max_total_records:
                break

            last_page_urls: set[str] | None = None

            # Fetch remaining pages if any.
            for page in range(2, max_pages + 1):
                listing_url = (
                    f"{base_url}/en/publications_and_press_releases/press/"
                    f"index_year_{year}-type_{type_value}-page_{page}.html"
                )

                if ctx.debug:
                    print(f"[{self.name}] Fetch {year} p{page} -> {listing_url}")

                try:
                    resp = _get_with_retries(
                        session,
                        listing_url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_jitter_seconds=backoff_jitter_seconds,
                    )
                except requests.HTTPError as e:
                    if getattr(e.response, "status_code", None) in (404,):
                        break
                    raise

                p = _DevbListingParser()
                p.feed(resp.text)

                page_detail_urls: set[str] = set()
                for row in p.rows:
                    if not row.href:
                        continue
                    abs_url = urljoin(listing_url, row.href)
                    if abs_url.startswith(base_url + "/en/"):
                        page_detail_urls.add(abs_url)

                if last_page_urls is not None and page_detail_urls and page_detail_urls == last_page_urls:
                    # Defensive stop: server returned same content for next page.
                    break
                if page_detail_urls:
                    last_page_urls = page_detail_urls

                before = len(out)
                _consume_rows(listing_url, p.rows)

                if len(out) == before and not p.rows:
                    # No data on this page.
                    break

                if len(out) >= max_total_records:
                    break

                delay = request_delay_seconds
                if request_jitter_seconds > 0:
                    delay += random.uniform(0.0, request_jitter_seconds)
                _sleep_seconds(delay)

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url, (r.meta.get("date") or "")))
        return out
