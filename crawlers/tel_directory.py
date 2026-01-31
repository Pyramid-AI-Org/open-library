from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord
from utils.html_links import extract_links


_TEL_HOST = "tel.directory.gov.hk"


_ABBREVIATIONS_JSON_PATH = Path(__file__).with_name("tel_directory_abbreviations.json")


_SKIP_DEPT_SEGMENTS = {
    "back",
    "help",
    "disclaimer",
    "copyright notice",
    "skip to content",
    "govhk",
}


_ALPHA_INDEX_SEGMENT_RE = re.compile(r"^(?:[A-Z]|[0-9])(?:\s*[-–]\s*(?:[A-Z]|[0-9]))*$")


def _load_tel_abbreviations() -> dict[str, str]:
    try:
        raw = json.loads(_ABBREVIATIONS_JSON_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}

    if not isinstance(raw, dict):
        return {}

    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        kk = " ".join(k.split()).strip()
        vv = " ".join(v.split()).strip()
        if kk and vv:
            out[kk] = vv
    return out


def _build_post_title_abbrev_expander(
    abbreviations: dict[str, str],
    *,
    require_capital_start: bool = True,
) -> tuple[re.Pattern[str] | None, dict[str, str]]:
    if not abbreviations:
        return None, {}

    selected: dict[str, str] = {}
    for short, long in abbreviations.items():
        if not short or not long:
            continue
        if require_capital_start and not short[0].isupper():
            continue
        selected[short] = long

    if not selected:
        return None, {}

    # Match abbreviations as standalone tokens (surrounded by non-alnum).
    # Sort by length desc so e.g. "Acct(s)" matches before "Acc".
    parts = sorted((re.escape(k) for k in selected.keys()), key=len, reverse=True)
    pat = re.compile(r"(?<![A-Za-z0-9])(" + "|".join(parts) + r")(?![A-Za-z0-9])")
    return pat, selected


def _expand_post_title_abbreviations(
    post_title: str | None,
    pattern: re.Pattern[str] | None,
    mapping: dict[str, str],
) -> tuple[str | None, list[dict[str, str]]]:
    t = (post_title or "").strip()
    if not t:
        return None, []
    if pattern is None or not mapping:
        return t, []

    used: list[dict[str, str]] = []
    seen: set[str] = set()

    def _repl(m: re.Match[str]) -> str:
        short = m.group(1)
        long = mapping.get(short)
        if long and short not in seen:
            seen.add(short)
            used.append({"short": short, "long": long})
        return long or short

    expanded = pattern.sub(_repl, t)
    return expanded, used


def _clean_department_segment(text: str) -> str | None:
    t = " ".join((text or "").split())
    if not t:
        return None

    # Many pages include A-Z / 0-9 index navigation. Those are not meaningful
    # department breadcrumb segments.
    if _ALPHA_INDEX_SEGMENT_RE.fullmatch(t):
        return None

    tl = t.lower()
    if tl in _SKIP_DEPT_SEGMENTS:
        return None
    if tl.startswith("skip to"):
        return None
    return t


def _path_to_key(path: list[str]) -> tuple[str, ...]:
    return tuple(path)


def _merge_department_path(meta: dict[str, Any], path: list[str]) -> None:
    if not path:
        return

    paths = meta.get("department_paths")
    if not isinstance(paths, list):
        meta["department_paths"] = [path]
        return

    # Ensure uniqueness while preserving insertion order.
    existing: set[tuple[str, ...]] = set()
    for p in paths:
        if isinstance(p, list) and all(isinstance(x, str) for x in p):
            existing.add(tuple(p))

    k = _path_to_key(path)
    if k not in existing:
        paths.append(path)
        existing.add(k)


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


def _canonicalize_tel_url(url: str) -> str | None:
    s = (url or "").strip()
    if not s:
        return None

    p = urlparse(s)
    if not p.scheme or not p.netloc:
        return None

    if p.netloc.lower() != _TEL_HOST:
        return None

    # Keep query (some links may carry state); drop fragments.
    p = p._replace(scheme=p.scheme.lower(), netloc=p.netloc.lower(), fragment="")

    # Normalize path slightly.
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    p = p._replace(path=path)

    return urlunparse(p)


def _canonicalize_any_url(url: str) -> str | None:
    s = (url or "").strip()
    if not s:
        return None

    lower = s.lower()
    if lower.startswith("javascript:"):
        return None

    p = urlparse(s)
    if not p.scheme or not p.netloc:
        return None

    p = p._replace(scheme=p.scheme.lower(), netloc=p.netloc.lower(), fragment="")
    return urlunparse(p)


def _is_crawlable_tel_page(url: str) -> bool:
    can = _canonicalize_tel_url(url)
    if not can:
        return False

    path = urlparse(can).path.lower()
    if not path.endswith("_eng.html"):
        return False

    # Skip helper/download instruction pages.
    if "/zipinstruct/" in path:
        return False

    return True


_PHONE_DIGITS_RE = re.compile(r"\d+")


def _normalize_phone(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return None

    digits = "".join(_PHONE_DIGITS_RE.findall(s))
    if not digits:
        return None

    # Heuristic: remove leading HK country code if present.
    if digits.startswith("852") and len(digits) > 8:
        digits = digits[3:]

    return digits or None


def _normalize_email(value: str) -> str | None:
    s = (value or "").strip().lower()
    if not s:
        return None
    if "@" not in s:
        return None
    return s


@dataclass
class _Cell:
    text: str
    href: str | None


@dataclass
class _Row:
    cells: list[_Cell]
    name_text: str | None
    name_href: str | None
    tel_text: str | None
    email_text: str | None


class _TableRowParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self._base_url = base_url

        self._table_stack: list[bool] = []
        self._in_result_table = False

        self._in_tr = False
        self._in_cell = False
        self._cell_text_parts: list[str] = []
        self._cell_href: str | None = None

        self._in_name_anchor = False
        self._in_tel_anchor = False
        self._in_email_anchor = False

        self._row_name_text_parts: list[str] = []
        self._row_name_href: str | None = None
        self._row_tel_text_parts: list[str] = []
        self._row_tel_text: str | None = None
        self._row_email_text_parts: list[str] = []
        self._row_email_text: str | None = None

        self._current_row: list[_Cell] = []
        self.rows: list[_Row] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()

        if t == "table":
            cls = ""
            for k, v in attrs:
                if k.lower() == "class" and v:
                    cls = v
                    break
            is_result = "result-table" in (cls or "").lower()
            self._table_stack.append(is_result)
            if is_result:
                self._in_result_table = True
            return

        if t == "tr":
            if not self._in_result_table:
                return
            self._in_tr = True
            self._current_row = []

            self._in_name_anchor = False
            self._in_tel_anchor = False
            self._in_email_anchor = False
            self._row_name_text_parts = []
            self._row_name_href = None
            self._row_tel_text_parts = []
            self._row_tel_text = None
            self._row_email_text_parts = []
            self._row_email_text = None
            return

        if not self._in_tr:
            return

        if t in ("td", "th"):
            self._in_cell = True
            self._cell_text_parts = []
            self._cell_href = None
            return

        if t == "a" and self._in_cell:
            href = None
            cls = ""
            for k, v in attrs:
                lk = k.lower()
                if lk == "href" and v:
                    href = v
                elif lk == "class" and v:
                    cls = v

            abs_href = urljoin(self._base_url, href) if href else None
            if abs_href:
                self._cell_href = abs_href

            cls_lower = (cls or "").lower()
            href_lower = (href or "").lower()

            # Mark up row-level fields (used to distinguish person rows from office contact rows).
            if "name" in cls_lower and abs_href:
                self._in_name_anchor = True
                self._row_name_href = abs_href
                self._row_name_text_parts = []
            elif (
                "tel" in cls_lower or href_lower.startswith("tel:")
            ) and self._row_tel_text is None:
                self._in_tel_anchor = True
                self._row_tel_text_parts = []
            elif (
                href_lower.startswith("mailto:") or "mail" in cls_lower
            ) and self._row_email_text is None:
                # Prefer the actual address from href if present.
                if href_lower.startswith("mailto:"):
                    email = (href or "")[len("mailto:") :].strip()
                    if email:
                        self._row_email_text = email
                self._in_email_anchor = True
                self._row_email_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "table":
            if self._table_stack:
                self._table_stack.pop()
            self._in_result_table = any(self._table_stack)
            return

        if t == "tr":
            if self._in_tr and self._current_row:
                name_text = " ".join("".join(self._row_name_text_parts).split()) or None

                tel_text = self._row_tel_text
                if tel_text is None:
                    tel_text = (
                        " ".join("".join(self._row_tel_text_parts).split()) or None
                    )
                email_text = self._row_email_text
                if email_text is None:
                    email_text = (
                        " ".join("".join(self._row_email_text_parts).split()) or None
                    )

                self.rows.append(
                    _Row(
                        cells=self._current_row,
                        name_text=name_text,
                        name_href=self._row_name_href,
                        tel_text=tel_text,
                        email_text=email_text,
                    )
                )
            self._in_tr = False
            self._in_cell = False
            self._cell_text_parts = []
            self._cell_href = None
            self._current_row = []
            return

        if t == "a":
            self._in_name_anchor = False
            self._in_tel_anchor = False
            self._in_email_anchor = False
            return

        if not self._in_tr:
            return

        if t in ("td", "th") and self._in_cell:
            text = " ".join("".join(self._cell_text_parts).split())
            self._current_row.append(_Cell(text=text, href=self._cell_href))
            self._in_cell = False
            self._cell_text_parts = []
            self._cell_href = None

    def handle_data(self, data: str) -> None:
        if self._in_tr and self._in_cell:
            self._cell_text_parts.append(data)

        if self._in_tr and self._in_name_anchor:
            self._row_name_text_parts.append(data)
        if self._in_tr and self._in_tel_anchor:
            self._row_tel_text_parts.append(data)
        if self._in_tr and self._in_email_anchor and self._row_email_text is None:
            self._row_email_text_parts.append(data)


def _extract_people_from_html(html: str, *, page_url: str) -> list[dict[str, Any]]:
    parser = _TableRowParser(base_url=page_url)
    parser.feed(html)

    people: list[dict[str, Any]] = []

    def _strip_prefix(text: str, prefix: str) -> str:
        t = (text or "").strip()
        if not t:
            return ""
        p = (prefix or "").strip()
        if not p:
            return t
        if t.lower().startswith(p.lower()):
            t = t[len(p) :].strip()
        return t

    for row in parser.rows:
        # Hard filter: only rows that contain a person link.
        if not row.name_href:
            continue

        cells = row.cells
        if len(cells) < 2:
            continue

        # Most person rows are: name | post title | office tel | email
        name = _strip_prefix((row.name_text or "").strip(), "Full Name")
        if not name:
            continue
        if name.strip().lower() == "(vacant)":
            continue

        post_title_raw = cells[1].text.strip() if len(cells) >= 2 else ""
        post_title = _strip_prefix(post_title_raw, "Post Title")

        office_tel_raw_full = (row.tel_text or "").strip()
        if not office_tel_raw_full and len(cells) >= 3:
            office_tel_raw_full = cells[2].text.strip()
        office_tel_raw = _strip_prefix(office_tel_raw_full, "Office Tel")

        email_raw_full = (row.email_text or "").strip()
        if not email_raw_full and len(cells) >= 4:
            email_raw_full = cells[3].text.strip()
        email_raw = _strip_prefix(email_raw_full, "Email")

        phone = _normalize_phone(office_tel_raw)
        if not phone:
            continue

        email = _normalize_email(email_raw)
        person_url = _canonicalize_any_url(row.name_href or "")

        people.append(
            {
                "name": name,
                "person_url": person_url,
                "post_title": post_title or None,
                "office_tel": office_tel_raw or None,
                "office_tel_norm": phone,
                "email": email,
            }
        )

    return people


class Crawler:
    """Crawl Government Telephone Directory (tel.directory.gov.hk) and emit one record per person.

    Traversal: start from index URL, follow department/subdepartment links within
    tel.directory.gov.hk, parse person rows from tables, and deduplicate by
    normalized office telephone number.

    Config: crawlers.tel_directory
      - index_url: https://tel.directory.gov.hk/index_ENG.html
      - request_delay_seconds: 0.25
      - request_jitter_seconds: 0.10
      - max_pages: 2000
      - max_total_records: 50000
      - backoff_base_seconds: 0.5
      - backoff_jitter_seconds: 0.25

    Uses shared http.timeout_seconds/user_agent/max_retries.
    """

    name = "tel_directory"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        index_url = str(
            cfg.get("index_url", f"https://{_TEL_HOST}/index_ENG.html")
        ).strip()
        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.10))

        max_pages = int(cfg.get("max_pages", 2000))
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

        abbreviations = _load_tel_abbreviations()
        post_title_abbrev_pattern, post_title_abbrev_map = (
            _build_post_title_abbrev_expander(abbreviations, require_capital_start=True)
        )

        start = _canonicalize_tel_url(index_url)
        if not start:
            return []

        discovered_at = datetime.now(timezone.utc).isoformat()

        @dataclass(frozen=True)
        class _QueueItem:
            url: str
            department_path: list[str]

        queue: list[_QueueItem] = [_QueueItem(url=start, department_path=[])]
        visited_pages: set[str] = set()

        # Track all known department paths for a page and the phones discovered on that page.
        page_paths: dict[str, set[tuple[str, ...]]] = {}
        page_phone_keys: dict[str, set[str]] = {}

        phone_to_record: dict[str, UrlRecord] = {}
        out: list[UrlRecord] = []

        def _ensure_page_path(url: str, dept_path: list[str]) -> None:
            if not dept_path:
                return
            paths = page_paths.setdefault(url, set())
            paths.add(_path_to_key(dept_path))

        def _merge_path_into_phone(phone_key: str, dept_path: list[str]) -> None:
            if not dept_path:
                return
            r = phone_to_record.get(phone_key)
            if not r:
                return
            if not isinstance(r.meta, dict):
                return
            _merge_department_path(r.meta, dept_path)

        while queue:
            item = queue.pop(0)
            page_url = item.url
            dept_path = item.department_path

            _ensure_page_path(page_url, dept_path)

            if page_url in visited_pages:
                continue
            if len(visited_pages) >= max_pages:
                break

            visited_pages.add(page_url)

            if ctx.debug:
                print(f"[{self.name}] Fetch {page_url}")

            resp = _get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )

            html = resp.text

            # 1) Extract people from tables
            people = _extract_people_from_html(html, page_url=page_url)
            phones_on_page: set[str] = set()
            for p in people:
                phone_key = str(p.get("office_tel_norm") or "").strip()
                if not phone_key:
                    continue

                phones_on_page.add(phone_key)

                url = str(p.get("person_url") or "").strip()
                if not url:
                    # Fall back to the page we discovered them on.
                    url = page_url

                existing = phone_to_record.get(phone_key)
                if existing is None:
                    post_title_short = p.get("post_title")
                    post_title_long, _post_title_abbrevs = (
                        _expand_post_title_abbreviations(
                            post_title_short,
                            post_title_abbrev_pattern,
                            post_title_abbrev_map,
                        )
                    )
                    # Ensure post_title_long is always populated when we have a post_title.
                    if post_title_short and (post_title_long is None or post_title_long == post_title_short):
                        post_title_long = post_title_short

                    meta: dict[str, Any] = {
                        "post_title": post_title_short,
                        "post_title_long": post_title_long,
                        "office_tel": p.get("office_tel"),
                        "email": p.get("email"),
                        "discovered_from": page_url,
                        "dedup_key": phone_key,
                        "department_paths": [],
                    }
                    _merge_department_path(meta, dept_path)

                    rec = UrlRecord(
                        url=url,
                        name=p.get("name") or None,
                        discovered_at_utc=discovered_at,
                        source=self.name,
                        meta=meta,
                    )
                    phone_to_record[phone_key] = rec
                    out.append(rec)
                else:
                    # Same phone encountered again: merge department paths and track other origins.
                    if isinstance(existing.meta, dict):
                        # Best-effort: fill in missing post_title/post_title_long from subsequent sightings.
                        post_title_short = p.get("post_title")
                        if post_title_short:
                            existing.meta.setdefault("post_title", post_title_short)
                            if existing.meta.get("post_title_long") is None:
                                post_title_long, _post_title_abbrevs = (
                                    _expand_post_title_abbreviations(
                                        post_title_short,
                                        post_title_abbrev_pattern,
                                        post_title_abbrev_map,
                                    )
                                )
                                if post_title_long is None or post_title_long == post_title_short:
                                    post_title_long = post_title_short
                                existing.meta["post_title_long"] = post_title_long

                        _merge_department_path(existing.meta, dept_path)
                        if isinstance(existing.meta.get("discovered_from"), str):
                            dfs = existing.meta.get("discovered_from_urls")
                            if not isinstance(dfs, list):
                                dfs = [existing.meta["discovered_from"]]
                                existing.meta["discovered_from_urls"] = dfs
                            if page_url not in dfs:
                                dfs.append(page_url)

                        # Optional: detect collisions where the URL differs.
                        if url and existing.url != url:
                            other_urls = existing.meta.get("other_person_urls")
                            if not isinstance(other_urls, list):
                                other_urls = []
                                existing.meta["other_person_urls"] = other_urls
                            if url not in other_urls and url != existing.url:
                                other_urls.append(url)

                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

            page_phone_keys[page_url] = phones_on_page

            # 2) Discover more office/department pages to crawl
            links = extract_links(html, base_url=page_url)
            for link in links:
                href = (link.href or "").strip()
                if not href:
                    continue
                if not _is_crawlable_tel_page(href):
                    continue

                can = _canonicalize_tel_url(href)
                if not can:
                    continue

                seg = _clean_department_segment(link.text or "")
                next_path = dept_path
                if seg:
                    if (
                        dept_path
                        and dept_path[-1].strip().lower() == seg.strip().lower()
                    ):
                        next_path = dept_path
                    else:
                        next_path = [*dept_path, seg]

                _ensure_page_path(can, next_path)

                if can in visited_pages:
                    # No refetch, but if we've already extracted phones from that page, merge in the new path.
                    for phone_key in page_phone_keys.get(can, set()):
                        _merge_path_into_phone(phone_key, next_path)
                    continue

                queue.append(_QueueItem(url=can, department_path=next_path))

            # Polite pacing
            delay = request_delay_seconds
            if request_jitter_seconds > 0:
                delay += random.uniform(0.0, request_jitter_seconds)
            _sleep_seconds(delay)

        out.sort(key=lambda r: (r.meta.get("dedup_key") or "", r.url))

        if ctx.debug:
            multi_path = 0
            for r in out:
                paths = (
                    r.meta.get("department_paths") if isinstance(r.meta, dict) else None
                )
                if isinstance(paths, list) and len(paths) > 1:
                    multi_path += 1
            print(f"[{self.name}] Unique phones: {len(out)}")
            print(f"[{self.name}] Phones with >1 department path: {multi_path}")

        return out
