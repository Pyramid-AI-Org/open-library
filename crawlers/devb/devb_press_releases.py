from __future__ import annotations

import random
import re
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

from crawlers.base import RunContext, UrlRecord, get_with_retries, sleep_seconds


_DETAIL_PATH_RE = re.compile(
    r"^/en/publications_and_press_releases/press/index_id_\d+\.html$", re.IGNORECASE
)


_DETAIL_PATH_RE_ANY_LOCALE = re.compile(
    r"^/(en|tc|sc)/publications_and_press_releases/press/index_id_\d+\.html$",
    re.IGNORECASE,
)


_DETAIL_ID_RE = re.compile(r"index_id_(\d+)\.html$", re.IGNORECASE)


def _is_chinese_only_title(title: str | None) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return False
    return "chinese only" in t or "chinese version" in t or "chinese online" in t


def _is_english_only_title(title: str | None) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return False
    return "english only" in t or "english version" in t or "english online" in t


def _rewrite_lang_url(url: str, *, base_url: str, lang: str) -> str:
    prefix = base_url.rstrip("/") + "/en/"
    if url.startswith(prefix):
        return base_url.rstrip("/") + f"/{lang}/" + url[len(prefix) :]
    return url


def _infer_locale_from_url(url: str, *, base_url: str) -> str | None:
    base = base_url.rstrip("/")
    for lang in ("en", "tc", "sc"):
        if url.startswith(base + f"/{lang}/"):
            return lang
    return None


def _infer_detail_id(url: str) -> str | None:
    path = (urlparse(url).path or "").strip()
    m = _DETAIL_ID_RE.search(path)
    return m.group(1) if m else None


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


def _apply_charset_fix(resp: requests.Response) -> None:
    # DevB pages sometimes omit charset and `requests` falls back to
    # ISO-8859-1 for `.text`, which breaks Chinese titles (mojibake).
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


_sleep_seconds = sleep_seconds


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


class _DevbDetailTitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.og_title: str | None = None
        self.title_text: str | None = None
        self.h1_text: str | None = None

        self._in_title = False
        self._title_parts: list[str] = []

        self._in_h1 = False
        self._h1_parts: list[str] = []

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

        if t == "meta" and not self.og_title:
            prop = (attrs_map.get("property") or "").lower()
            if prop == "og:title":
                content = (attrs_map.get("content") or "").strip()
                if content:
                    self.og_title = content

        if t == "title":
            self._in_title = True
            self._title_parts = []
            return

        if t == "h1" and not self.h1_text:
            self._in_h1 = True
            self._h1_parts = []
            return

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t == "title" and self._in_title:
            self._in_title = False
            txt = "".join(self._title_parts).strip()
            self.title_text = txt or None
            return

        if t == "h1" and self._in_h1:
            self._in_h1 = False
            txt = "".join(self._h1_parts).strip()
            self.h1_text = txt or None
            return

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_parts.append(data)
        if self._in_h1:
            self._h1_parts.append(data)

    def best_title(self) -> str | None:
        # Prefer explicit OpenGraph title, then H1, then <title>.
        return self.og_title or self.h1_text or self.title_text


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
        chinese_only_lang = str(cfg.get("chinese_only_lang", "tc")).strip().lower()
        if chinese_only_lang not in ("tc", "sc"):
            chinese_only_lang = "tc"

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

        detail_title_cache: dict[str, str | None] = {}

        for year in range(end_year, start_year - 1, -1):
            # Fetch first page to discover max pages.
            first_url = (
                f"{base_url}/en/publications_and_press_releases/press/"
                f"index_year_{year}-type_{type_value}-page_1.html"
            )

            first_url_zh = _rewrite_lang_url(
                first_url, base_url=base_url, lang=chinese_only_lang
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

            zh_parser = _DevbListingParser()
            try:
                zh_resp = _get_with_retries(
                    session,
                    first_url_zh,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                zh_parser.feed(zh_resp.text)
            except requests.HTTPError as e:
                # Some years/locales may not exist.
                if getattr(e.response, "status_code", None) not in (404,):
                    raise

            inferred_max = _infer_max_pages_from_html(
                first_resp.text, year=year, type_value=type_value
            )
            max_pages = parser.max_pages or inferred_max or 1

            def _index_rows_by_id(
                listing_url: str, rows: list[_ListingRow]
            ) -> dict[str, _ListingRow]:
                out_rows: dict[str, _ListingRow] = {}
                for row in rows:
                    if not row.href:
                        continue
                    abs_url = urljoin(listing_url, row.href)
                    if not _DETAIL_PATH_RE_ANY_LOCALE.match(abs_url[len(base_url) :]):
                        continue
                    pr_id = _infer_detail_id(abs_url)
                    if not pr_id:
                        continue
                    out_rows[pr_id] = row
                return out_rows

            def _get_localized_title(
                *,
                pr_id: str | None,
                lang: str,
                en_row: _ListingRow,
                zh_rows_by_id: dict[str, _ListingRow],
            ) -> str | None:
                if lang == "en":
                    return en_row.title
                if pr_id and pr_id in zh_rows_by_id:
                    return zh_rows_by_id[pr_id].title or en_row.title
                return en_row.title

            def _fetch_detail_title(url: str) -> str | None:
                if url in detail_title_cache:
                    return detail_title_cache[url]
                try:
                    resp = _get_with_retries(
                        session,
                        url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_jitter_seconds=backoff_jitter_seconds,
                    )
                except requests.RequestException:
                    detail_title_cache[url] = None
                    return None

                p = _DevbDetailTitleParser()
                p.feed(resp.text)
                title = p.best_title()
                detail_title_cache[url] = title
                return title

            def _get_localized_date_iso(
                *,
                pr_id: str | None,
                lang: str,
                en_row: _ListingRow,
                zh_rows_by_id: dict[str, _ListingRow],
            ) -> str | None:
                if lang == "en":
                    return en_row.date_iso
                if pr_id and pr_id in zh_rows_by_id:
                    return zh_rows_by_id[pr_id].date_iso or en_row.date_iso
                return en_row.date_iso

            def _consume_rows(
                listing_url_en: str,
                rows_en: list[_ListingRow],
                listing_url_zh: str,
                zh_rows_by_id: dict[str, _ListingRow],
            ) -> int:
                added = 0
                for row in rows_en:
                    if not row.href:
                        continue

                    abs_url = urljoin(listing_url_en, row.href)

                    # English only + expected detail path.
                    if not abs_url.startswith(base_url + "/en/"):
                        continue

                    path = abs_url[len(base_url) :]
                    if not _DETAIL_PATH_RE.match(path):
                        continue

                    pr_id = _infer_detail_id(abs_url)

                    # URL selection policy:
                    # - (Chinese only) => store the Chinese URL
                    # - (English only) => store the English URL
                    # - otherwise => store BOTH English and Traditional Chinese
                    urls_to_add: list[str] = []
                    if _is_chinese_only_title(row.title):
                        if (
                            pr_id
                            and pr_id in zh_rows_by_id
                            and zh_rows_by_id[pr_id].href
                        ):
                            urls_to_add = [
                                urljoin(listing_url_zh, zh_rows_by_id[pr_id].href or "")
                            ]
                        else:
                            urls_to_add = [
                                _rewrite_lang_url(
                                    abs_url, base_url=base_url, lang=chinese_only_lang
                                )
                            ]
                    elif _is_english_only_title(row.title):
                        urls_to_add = [abs_url]
                    else:
                        zh_url: str = _rewrite_lang_url(
                            abs_url, base_url=base_url, lang=chinese_only_lang
                        )
                        if (
                            pr_id
                            and pr_id in zh_rows_by_id
                            and zh_rows_by_id[pr_id].href
                        ):
                            zh_url = urljoin(
                                listing_url_zh, zh_rows_by_id[pr_id].href or ""
                            )
                        urls_to_add = [abs_url, zh_url]

                    for final_url in urls_to_add:
                        if final_url in seen_urls:
                            continue
                        seen_urls.add(final_url)

                        locale = _infer_locale_from_url(final_url, base_url=base_url)
                        localized_title = _get_localized_title(
                            pr_id=pr_id,
                            lang=locale or "en",
                            en_row=row,
                            zh_rows_by_id=zh_rows_by_id,
                        )
                        if (locale and locale != "en") and (
                            not localized_title
                            or (pr_id and pr_id not in zh_rows_by_id)
                        ):
                            localized_title = (
                                _fetch_detail_title(final_url) or localized_title
                            )
                        localized_date_iso = _get_localized_date_iso(
                            pr_id=pr_id,
                            lang=locale or "en",
                            en_row=row,
                            zh_rows_by_id=zh_rows_by_id,
                        )
                        localized_listing_url = listing_url_en
                        if locale and locale != "en":
                            localized_listing_url = listing_url_zh

                        out.append(
                            UrlRecord(
                                url=final_url,
                                name=localized_title,
                                discovered_at_utc=discovered_at,
                                source=self.name,
                                meta={
                                    "date": localized_date_iso,
                                    "year": year,
                                    "listing_url": localized_listing_url,
                                    "locale": locale,
                                },
                            )
                        )
                        added += 1

                        if len(out) >= max_total_records:
                            break

                    if len(out) >= max_total_records:
                        break
                return added

            zh_rows_by_id_first = _index_rows_by_id(first_url_zh, zh_parser.rows)
            _consume_rows(first_url, parser.rows, first_url_zh, zh_rows_by_id_first)
            if len(out) >= max_total_records:
                break

            last_page_urls: set[str] | None = None

            # Fetch remaining pages if any.
            for page in range(2, max_pages + 1):
                listing_url = (
                    f"{base_url}/en/publications_and_press_releases/press/"
                    f"index_year_{year}-type_{type_value}-page_{page}.html"
                )

                listing_url_zh = _rewrite_lang_url(
                    listing_url, base_url=base_url, lang=chinese_only_lang
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

                p_zh = _DevbListingParser()
                try:
                    resp_zh = _get_with_retries(
                        session,
                        listing_url_zh,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_jitter_seconds=backoff_jitter_seconds,
                    )
                    p_zh.feed(resp_zh.text)
                except requests.HTTPError as e:
                    if getattr(e.response, "status_code", None) not in (404,):
                        raise

                page_detail_urls: set[str] = set()
                for row in p.rows:
                    if not row.href:
                        continue
                    abs_url = urljoin(listing_url, row.href)
                    if abs_url.startswith(base_url + "/en/"):
                        page_detail_urls.add(abs_url)

                if (
                    last_page_urls is not None
                    and page_detail_urls
                    and page_detail_urls == last_page_urls
                ):
                    # Defensive stop: server returned same content for next page.
                    break
                if page_detail_urls:
                    last_page_urls = page_detail_urls

                before = len(out)
                zh_rows_by_id = _index_rows_by_id(listing_url_zh, p_zh.rows)
                _consume_rows(listing_url, p.rows, listing_url_zh, zh_rows_by_id)

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
