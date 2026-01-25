from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord
from utils.html_links import extract_links


_TEL_HOST = "tel.directory.gov.hk"


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

        start = _canonicalize_tel_url(index_url)
        if not start:
            return []

        discovered_at = datetime.now(timezone.utc).isoformat()

        queue: list[str] = [start]
        visited_pages: set[str] = set()

        seen_phones: set[str] = set()
        out: list[UrlRecord] = []

        while queue:
            page_url = queue.pop(0)

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
            for p in people:
                phone_key = str(p.get("office_tel_norm") or "").strip()
                if not phone_key:
                    continue
                if phone_key in seen_phones:
                    continue
                seen_phones.add(phone_key)

                url = str(p.get("person_url") or "").strip()
                if not url:
                    # Fall back to the page we discovered them on.
                    url = page_url

                out.append(
                    UrlRecord(
                        url=url,
                        name=p.get("name") or None,
                        discovered_at_utc=discovered_at,
                        source=self.name,
                        meta={
                            "post_title": p.get("post_title"),
                            "office_tel": p.get("office_tel"),
                            "email": p.get("email"),
                            "discovered_from": page_url,
                            "dedup_key": phone_key,
                        },
                    )
                )

                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

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
                if can in visited_pages:
                    continue
                queue.append(can)

            # Polite pacing
            delay = request_delay_seconds
            if request_jitter_seconds > 0:
                delay += random.uniform(0.0, request_jitter_seconds)
            _sleep_seconds(delay)

        out.sort(key=lambda r: (r.meta.get("dedup_key") or "", r.url))
        return out
