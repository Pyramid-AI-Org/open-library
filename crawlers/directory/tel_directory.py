from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord, get_with_retries, sleep_seconds
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


def _normalize_department_id(value: str | None) -> str | None:
    t = " ".join((value or "").split()).strip()
    return t or None


def _effective_department_path(
    dept_path: list[str],
    *,
    is_enquiry_like: bool,
    department_root: str | None,
) -> list[str]:
    if not dept_path and not department_root:
        return []

    if is_enquiry_like:
        # Enquiry-like contacts repeat across many sub-offices; pin them to bureau root.
        root = _normalize_department_id(department_root)
        if root:
            return [root]
        return [dept_path[0]] if dept_path else []

    return dept_path


def _department_id_from_path(path: list[str]) -> str | None:
    if not path:
        return None
    # Use the full breadcrumb as the department identifier so that
    # (phone, department) uniquely identifies a record.
    return _normalize_department_id(" -> ".join(path))


def _dedup_key(phone_norm: str, department_id: str | None) -> str:
    d = _normalize_department_id(department_id or "") or ""
    # Stable, human-readable composite key.
    return f"{phone_norm}|{d}"


def _set_department_path(meta: dict[str, Any], path: list[str]) -> None:
    # Keep the schema stable: always a list of breadcrumb lists.
    if path:
        meta["department_paths"] = [path]
    else:
        meta["department_paths"] = []


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
    )


_sleep_seconds = sleep_seconds


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


def _extract_office_tree_paths(html: str, *, page_url: str) -> dict[str, list[str]]:
    """Parse the nested "Please select office" tree and return {page_url: full_path}.

    This is more faithful than building department paths from crawl traversal, because
    the site may link to a deep office page directly while still showing intermediate
    hierarchy levels only in this tree.
    """

    class _OfficeTreeParser(HTMLParser):
        def __init__(self, *, base_url: str) -> None:
            super().__init__()
            self._base_url = base_url

            self._in_whole_list = False
            self._whole_list_div_depth = 0
            self._ul_depth = 0

            self._in_a = False
            self._a_href: str | None = None
            self._a_text_parts: list[str] = []
            self._a_ul_depth: int = 0

            self._path_by_depth: list[str] = []
            self.paths: dict[str, list[str]] = {}

        def handle_starttag(
            self, tag: str, attrs: list[tuple[str, str | None]]
        ) -> None:
            t = tag.lower()

            if t == "div":
                cls = ""
                for k, v in attrs:
                    if k.lower() == "class" and v:
                        cls = v
                        break
                if not self._in_whole_list and "whole-list" in (cls or "").lower():
                    self._in_whole_list = True
                    self._whole_list_div_depth = 1
                    return
                if self._in_whole_list:
                    self._whole_list_div_depth += 1

            if not self._in_whole_list:
                return

            if t == "ul":
                self._ul_depth += 1
                return

            if t == "a":
                href = None
                for k, v in attrs:
                    if k.lower() == "href" and v:
                        href = v
                        break
                if not href:
                    return

                abs_href = urljoin(self._base_url, href)
                can = _canonicalize_tel_url(abs_href)
                if not can:
                    return
                if not _is_crawlable_tel_page(can):
                    return

                self._in_a = True
                self._a_href = can
                self._a_text_parts = []
                self._a_ul_depth = self._ul_depth

        def handle_endtag(self, tag: str) -> None:
            t = tag.lower()

            if t == "div" and self._in_whole_list:
                self._whole_list_div_depth -= 1
                if self._whole_list_div_depth <= 0:
                    self._in_whole_list = False
                    self._whole_list_div_depth = 0
                    self._ul_depth = 0
                    self._path_by_depth = []
                return

            if not self._in_whole_list:
                return

            if t == "ul":
                self._ul_depth = max(0, self._ul_depth - 1)
                if len(self._path_by_depth) > self._ul_depth:
                    self._path_by_depth = self._path_by_depth[: self._ul_depth]
                return

            if t == "a" and self._in_a:
                label = " ".join("".join(self._a_text_parts).split()).strip()
                if label and self._a_href and self._a_ul_depth > 0:
                    depth = self._a_ul_depth
                    if len(self._path_by_depth) < depth:
                        self._path_by_depth.extend(
                            [""] * (depth - len(self._path_by_depth))
                        )
                    self._path_by_depth[depth - 1] = label
                    self._path_by_depth = self._path_by_depth[:depth]
                    self.paths[self._a_href] = [p for p in self._path_by_depth if p]

                self._in_a = False
                self._a_href = None
                self._a_text_parts = []
                self._a_ul_depth = 0

        def handle_data(self, data: str) -> None:
            if self._in_a:
                self._a_text_parts.append(data)

    p = _OfficeTreeParser(base_url=page_url)
    p.feed(html)
    return p.paths


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
    kind: str  # "people" | "service"
    cells: list[_Cell]
    name_text: str | None
    name_href: str | None
    tel_text: str | None
    email_text: str | None


class _TableRowParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self._base_url = base_url

        # Track nested tables and whether they contain rows we care about.
        # Values: "people" | "service" | "other"
        self._table_stack: list[str] = []
        self._active_table_kind: str | None = None

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
            cls_lower = (cls or "").lower()
            kind = "other"
            if "result-table" in cls_lower:
                # We only care about the two main tabular result blocks.
                if "full-list-service" in cls_lower:
                    kind = "service"
                elif "full-list" in cls_lower:
                    kind = "people"
            self._table_stack.append(kind)
            self._active_table_kind = next(
                (k for k in reversed(self._table_stack) if k in ("people", "service")),
                None,
            )
            return

        if t == "tr":
            if self._active_table_kind not in ("people", "service"):
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
            self._active_table_kind = next(
                (k for k in reversed(self._table_stack) if k in ("people", "service")),
                None,
            )
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
                        kind=self._active_table_kind or "people",
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

    def _service_department_from_cell(text: str) -> str | None:
        # In "service" tables, the 2nd cell is:
        # "Bureau / Department / Related Organisation <NAME>"
        t = " ".join((text or "").split()).strip()
        if not t:
            return None
        t = _strip_prefix(t, "Bureau / Department / Related Organisation")
        t = " ".join(t.split()).strip()
        return t or None

    def _is_enquiry_like(
        name: str, *, post_title: str | None, href: str | None
    ) -> bool:
        nl = " ".join((name or "").split()).strip().lower()
        if nl in {"enquiry", "general enquiry", "general inquiry", "enquiries"}:
            return True
        pt = " ".join((post_title or "").split()).strip().lower()
        if pt in {"-", "enquiry", "general enquiry"}:
            return True
        hl = (href or "").lower()
        if "service_details.jsp" in hl or "service" in hl:
            return True
        return False

    for row in parser.rows:
        # Hard filter: only rows that contain a person link.
        if not row.name_href:
            continue

        cells = row.cells
        if len(cells) < 2:
            continue

        is_service = (
            row.kind == "service"
            or "service_details.jsp" in (row.name_href or "").lower()
        )

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
                "is_enquiry_like": _is_enquiry_like(
                    name,
                    post_title=post_title or None,
                    href=row.name_href,
                )
                or is_service,
                "department_override": (
                    _service_department_from_cell(post_title_raw)
                    if is_service
                    else None
                ),
            }
        )

    return people


class Crawler:
    """Crawl Government Telephone Directory (tel.directory.gov.hk).

    Traversal: start from index URL, follow department/subdepartment links within
    tel.directory.gov.hk, parse person/service rows from tables, and deduplicate
    by (normalized office telephone, department).

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
        page_entries: dict[str, list[dict[str, Any]]] = {}

        record_by_key: dict[str, UrlRecord] = {}
        out: list[UrlRecord] = []

        def _post_title_long_from_short(post_title_short: str | None) -> str | None:
            long, _used = _expand_post_title_abbreviations(
                post_title_short,
                post_title_abbrev_pattern,
                post_title_abbrev_map,
            )
            if post_title_short and (long is None or long == post_title_short):
                return post_title_short
            return long

        def _record_department_root(
            *, dept_override: str | None, dept_path: list[str]
        ) -> str | None:
            if dept_override:
                return _normalize_department_id(dept_override)
            if dept_path:
                return _normalize_department_id(dept_path[0])
            return None

        def _department_id(
            *, is_enquiry_like: bool, department_root: str | None, dept_path: list[str]
        ) -> str | None:
            if is_enquiry_like:
                return _normalize_department_id(department_root)
            return _department_id_from_path(dept_path)

        def _make_meta(
            *,
            discovered_from: str,
            dedup_key: str,
            department_root: str | None,
            dept_path: list[str],
            post_title_short: str | None,
            post_title_long: str | None,
            office_tel: str | None,
            email: str | None,
        ) -> dict[str, Any]:
            meta: dict[str, Any] = {
                "department_root": department_root,
                "post_title": post_title_short,
                "post_title_long": post_title_long,
                "office_tel": office_tel,
                "email": email,
                "discovered_from": discovered_from,
                "dedup_key": dedup_key,
            }
            _set_department_path(meta, dept_path)
            return meta

        def _ensure_record_for_page_path(page_url: str, dept_path: list[str]) -> None:
            entries = page_entries.get(page_url)
            if not entries:
                return

            for e in entries:
                phone_key = str(e.get("phone_key") or "").strip()
                if not phone_key:
                    continue

                is_enquiry_like = bool(e.get("is_enquiry_like"))
                department_root = _normalize_department_id(e.get("department_root"))
                effective_dept_path = _effective_department_path(
                    dept_path,
                    is_enquiry_like=is_enquiry_like,
                    department_root=department_root,
                )

                dept_id = _department_id(
                    is_enquiry_like=is_enquiry_like,
                    department_root=department_root,
                    dept_path=effective_dept_path,
                )
                k = _dedup_key(phone_key, dept_id)
                if k in record_by_key:
                    continue

                meta = _make_meta(
                    discovered_from=page_url,
                    dedup_key=k,
                    department_root=department_root,
                    dept_path=effective_dept_path,
                    post_title_short=e.get("post_title"),
                    post_title_long=e.get("post_title_long"),
                    office_tel=e.get("office_tel"),
                    email=e.get("email"),
                )

                rec = UrlRecord(
                    url=str(e.get("url") or page_url),
                    name=e.get("name") or None,
                    discovered_at_utc=discovered_at,
                    source=self.name,
                    meta=meta,
                )
                record_by_key[k] = rec
                out.append(rec)

        while queue:
            item = queue.pop(0)
            page_url = item.url
            dept_path = item.department_path

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

            # Prefer canonical department paths from the office tree on the page.
            office_tree = _extract_office_tree_paths(html, page_url=page_url)
            dept_path_from_tree = office_tree.get(page_url)
            if isinstance(dept_path_from_tree, list) and all(
                isinstance(x, str) for x in dept_path_from_tree
            ):
                dept_path = dept_path_from_tree

            # 1) Extract people from tables
            people = _extract_people_from_html(html, page_url=page_url)
            entries_on_page: list[dict[str, Any]] = []
            for p in people:
                phone_key = str(p.get("office_tel_norm") or "").strip()
                if not phone_key:
                    continue

                url = str(p.get("person_url") or "").strip()
                if not url:
                    # Fall back to the page we discovered them on.
                    url = page_url

                is_enquiry_like = bool(p.get("is_enquiry_like"))
                dept_override = p.get("department_override")
                department_root = _record_department_root(
                    dept_override=(
                        dept_override if isinstance(dept_override, str) else None
                    ),
                    dept_path=dept_path,
                )

                effective_dept_path = _effective_department_path(
                    dept_path,
                    is_enquiry_like=is_enquiry_like,
                    department_root=department_root,
                )

                dept_id = _department_id(
                    is_enquiry_like=is_enquiry_like,
                    department_root=department_root,
                    dept_path=effective_dept_path,
                )
                k = _dedup_key(phone_key, dept_id)

                post_title_short = p.get("post_title")
                post_title_long = _post_title_long_from_short(post_title_short)

                # Persist enough information to mint additional records if this page
                # is later discovered under a different department path.
                entries_on_page.append(
                    {
                        "phone_key": phone_key,
                        "url": url,
                        "name": p.get("name") or None,
                        "is_enquiry_like": is_enquiry_like,
                        "department_root": department_root,
                        "post_title": post_title_short,
                        "post_title_long": post_title_long,
                        "office_tel": p.get("office_tel"),
                        "email": p.get("email"),
                    }
                )

                existing = record_by_key.get(k)
                if existing is None:
                    meta = _make_meta(
                        discovered_from=page_url,
                        dedup_key=k,
                        department_root=department_root,
                        dept_path=effective_dept_path,
                        post_title_short=post_title_short,
                        post_title_long=post_title_long,
                        office_tel=p.get("office_tel"),
                        email=p.get("email"),
                    )

                    rec = UrlRecord(
                        url=url,
                        name=p.get("name") or None,
                        discovered_at_utc=discovered_at,
                        source=self.name,
                        meta=meta,
                    )
                    record_by_key[k] = rec
                    out.append(rec)
                else:
                    # Same (phone, department) encountered again: keep a single record.
                    if isinstance(existing.meta, dict) and isinstance(
                        existing.meta.get("discovered_from"), str
                    ):
                        dfs = existing.meta.get("discovered_from_urls")
                        if not isinstance(dfs, list):
                            dfs = [existing.meta["discovered_from"]]
                            existing.meta["discovered_from_urls"] = dfs
                        if page_url not in dfs:
                            dfs.append(page_url)

                    if url and existing.url != url and isinstance(existing.meta, dict):
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

            page_entries[page_url] = entries_on_page

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

                # If the office tree on the current page knows the full path for the target page,
                # use that as the canonical next_path.
                tree_path = office_tree.get(can)
                if isinstance(tree_path, list) and all(
                    isinstance(x, str) for x in tree_path
                ):
                    next_path = tree_path
                else:
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

                if can in visited_pages:
                    # No refetch, but if we've already extracted entries from that page,
                    # create additional records for the new department path.
                    _ensure_record_for_page_path(can, next_path)
                    continue

                queue.append(_QueueItem(url=can, department_path=next_path))

            # Polite pacing
            delay = request_delay_seconds
            if request_jitter_seconds > 0:
                delay += random.uniform(0.0, request_jitter_seconds)
            _sleep_seconds(delay)

        out.sort(key=lambda r: (r.meta.get("dedup_key") or "", r.url))

        if ctx.debug:
            print(f"[{self.name}] Unique records: {len(out)}")

        return out
