from __future__ import annotations

from datetime import date, datetime
import json
import re
from typing import Any
from urllib.parse import urljoin

import requests

from crawlers.base import canonicalize_url, infer_name_from_link, path_ext

_ALLOWED_DOC_EXTS = {".pdf"}


def canonicalize_legco_url(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def parse_run_year(run_date_utc: str) -> int:
    return date.fromisoformat(run_date_utc).year


def term_from_year(year: int) -> str:
    if year < 2022:
        return "07"
    term = int((year - 2022) / 4) + 7
    return f"{term:02d}"


def iso_from_dot_date(value: str) -> str | None:
    s = (value or "").strip()
    if not s:
        return None

    parts = s.split(".")
    if len(parts) != 3:
        return None

    try:
        y = int(parts[0])
        m = int(parts[1])
        d = int(parts[2])
        return date(y, m, d).isoformat()
    except ValueError:
        return None


def display_from_iso_date(value: str | None) -> str | None:
    if not value:
        return None

    try:
        dt = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None

    month_name = dt.strftime("%B")
    return f"{dt.day} {month_name} {dt.year}"


def display_from_iso_date_localized(value: str | None, locale: str) -> str | None:
    if not value:
        return None

    try:
        dt = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None

    if locale == "tc":
        return f"{dt.year}年{dt.month}月{dt.day}日"

    month_name = dt.strftime("%B")
    return f"{dt.day} {month_name} {dt.year}"


def build_meeting_name(committee_en: str, committee_tc: str, locale: str, date_text: str) -> str:
    if locale == "tc":
        return f"{committee_tc}會議，{date_text}"

    return f"{committee_en} Meeting, {date_text}"


def extract_meeting_token(row: dict[str, Any], publish_iso: str | None) -> str | None:
    for key in ("minutes", "agenda"):
        val = row.get(key)
        if not isinstance(val, dict):
            continue

        for lang_key in ("en", "tc"):
            href = str(val.get(lang_key, "")).strip()
            if not href:
                continue

            tail = href.rsplit("/", 1)[-1]
            stem = tail.rsplit(".", 1)[0]
            digits = "".join(ch for ch in stem if ch.isdigit())
            if len(digits) < 8:
                continue

            token = digits[-8:]
            suffix = stem[len(stem) - 1]
            if suffix.isalpha() and len(stem) > 8:
                return token + suffix.lower()
            return token

    if publish_iso:
        return publish_iso.replace("-", "")

    return None


def pick_title_for_pdf(pdf_url: str, title_obj: dict[str, Any]) -> str | None:
    en = str(title_obj.get("en", "")).strip()
    tc = str(title_obj.get("tc", "")).strip()

    lower = (pdf_url or "").lower()
    if "/chinese/" in lower:
        return tc or en or None

    if "/english/" in lower:
        return en or tc or None

    return en or tc or None


def locale_from_url(url: str, fallback: str = "en") -> str:
    lower = (url or "").lower()
    if "/chinese/" in lower or "/tc/" in lower:
        return "tc"
    if "/english/" in lower or "/en/" in lower:
        return "en"
    return fallback


def response_json(resp: requests.Response) -> dict[str, Any]:
    raw = resp.content.decode("utf-8-sig")
    loaded = json.loads(raw)
    return loaded if isinstance(loaded, dict) else {}


def iter_bilingual_pdf_documents(
    *,
    base_url: str,
    pdf_obj: dict[str, Any],
    title_obj: dict[str, Any] | None,
    seen_urls: set[str],
) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    title_data = title_obj if isinstance(title_obj, dict) else {}

    for lang_key in ("en", "tc"):
        pdf_href = str(pdf_obj.get(lang_key, "")).strip()
        if not pdf_href:
            continue

        abs_url = canonicalize_legco_url(urljoin(base_url + "/", pdf_href))
        if not abs_url:
            continue
        if path_ext(abs_url) not in _ALLOWED_DOC_EXTS:
            continue
        if abs_url in seen_urls:
            continue

        record_locale = locale_from_url(abs_url, fallback=lang_key)
        paper_name = pick_title_for_pdf(abs_url, title_data)
        if not paper_name:
            paper_name = infer_name_from_link(None, abs_url)

        seen_urls.add(abs_url)
        out.append((abs_url, record_locale, paper_name))

    return out


def extract_panel_code(committee_file_path: str | None) -> str | None:
    path = str(committee_file_path or "").strip()
    if not path:
        return None

    m = re.search(r"/panels/([^/]+)/", path)
    if not m:
        m = re.search(r"/(?:english|chinese)/([^/]+)/([^/]+)/", path)
        if not m:
            return None
        return m.group(2).strip() or None

    code = m.group(1).strip()
    return code or None


def build_minutes_fallback_url(
    *,
    base_url: str,
    committee_file_path: str,
    panel_code: str,
    meeting_token: str,
) -> str:
    base_path = committee_file_path.strip()
    if not base_path.endswith("/"):
        base_path += "/"

    rel = f"{base_path}minutes/{panel_code}{meeting_token}.pdf"
    return urljoin(base_url + "/", rel)


def url_exists(session: requests.Session, url: str, *, timeout_seconds: int) -> bool:
    try:
        head = session.head(url, timeout=timeout_seconds, allow_redirects=True)
        if head.status_code == 200:
            return True
        if head.status_code != 405:
            return False
    except requests.RequestException:
        return False

    try:
        resp = session.get(url, timeout=timeout_seconds, stream=True)
        try:
            return resp.status_code == 200
        finally:
            resp.close()
    except requests.RequestException:
        return False