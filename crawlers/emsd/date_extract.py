from __future__ import annotations

from html import unescape
import re
from urllib.parse import unquote, urlparse


_MONTH_TO_NUMBER: dict[str, str] = {
    "jan": "01",
    "january": "01",
    "feb": "02",
    "february": "02",
    "mar": "03",
    "march": "03",
    "apr": "04",
    "april": "04",
    "may": "05",
    "jun": "06",
    "june": "06",
    "jul": "07",
    "july": "07",
    "aug": "08",
    "august": "08",
    "sep": "09",
    "sept": "09",
    "september": "09",
    "oct": "10",
    "october": "10",
    "nov": "11",
    "november": "11",
    "dec": "12",
    "december": "12",
}

_MONTH_TOKEN = (
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?"
)

_FULL_DATE_RE = re.compile(
    rf"\b(\d{{1,2}})\s+({_MONTH_TOKEN})\s*,?\s*(\d{{4}})\b",
    re.IGNORECASE,
)
_MONTH_YEAR_RE = re.compile(
    rf"\b({_MONTH_TOKEN})\s+(\d{{4}})\b",
    re.IGNORECASE,
)
_YEAR_EDITION_RE = re.compile(r"\b(\d{4})\s+edition\b", re.IGNORECASE)
_ISSUED_IN_RE = re.compile(
    rf"\bissued\s+in\s+({_MONTH_TOKEN})\s+(\d{{4}})\b",
    re.IGNORECASE,
)
_LAST_UPDATE_RE = re.compile(
    rf"\blast\s+update\s*:\s*({_MONTH_TOKEN})\s+(\d{{4}})\b",
    re.IGNORECASE,
)
_PUBLISHED_ON_RE = re.compile(
    rf"\bpublished\s+on\s+(\d{{1,2}})\s+({_MONTH_TOKEN})\s*,?\s*(\d{{4}})\b",
    re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")
_TAG_RE = re.compile(r"<[^>]+>")


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    text = unescape(value)
    text = _TAG_RE.sub(" ", text)
    return _WS_RE.sub(" ", text).strip()


def _month_number(token: str) -> str | None:
    return _MONTH_TO_NUMBER.get((token or "").strip().lower())


def _to_iso(*, year: str, month: str, day: str) -> str:
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def extract_publish_date_from_text(
    *texts: str, include_edition: bool = True
) -> str | None:
    def _parse_one(text: str) -> str | None:
        s = _normalize_text(text)
        if not s:
            return None

        # Prefer explicit full-day publication date when available.
        for rx in (_PUBLISHED_ON_RE, _FULL_DATE_RE):
            matches = list(rx.finditer(s))
            if not matches:
                continue
            day, month_tok, year = matches[-1].groups()
            month = _month_number(month_tok)
            if month:
                return _to_iso(year=year, month=month, day=day)

        for rx in (_LAST_UPDATE_RE, _ISSUED_IN_RE, _MONTH_YEAR_RE):
            matches = list(rx.finditer(s))
            if not matches:
                continue
            month_tok, year = matches[-1].groups()
            month = _month_number(month_tok)
            if month:
                return _to_iso(year=year, month=month, day="01")

        if include_edition:
            edition_matches = list(_YEAR_EDITION_RE.finditer(s))
            if edition_matches:
                year = edition_matches[-1].group(1)
                return _to_iso(year=year, month="01", day="01")

        return None

    for text in texts:
        parsed = _parse_one(text)
        if parsed:
            return parsed

    return None


def extract_publish_date_near_href(
    *, html: str, href: str, link_text: str
) -> str | None:
    """Best-effort extraction from link text plus nearby HTML around href."""
    candidates: list[str] = [link_text or ""]

    href_token = (href or "").strip()
    if href_token:
        candidates.append(href_token)

        path = (urlparse(href_token).path or "").strip()
        if path:
            candidates.append(path)
            basename = path.rsplit("/", 1)[-1]
            if basename:
                candidates.append(unquote(basename))

        source = html or ""
        source_l = source.lower()
        token_variants = [href_token, unquote(href_token)]
        if path:
            token_variants.extend([path, unquote(path)])
            base = path.rsplit("/", 1)[-1]
            if base:
                token_variants.extend([base, unquote(base)])

        seen_variants: set[str] = set()
        for raw_token in token_variants:
            token = (raw_token or "").strip()
            if not token:
                continue
            token_l = token.lower()
            if token_l in seen_variants:
                continue
            seen_variants.add(token_l)

            idx = source_l.find(token_l)
            if idx < 0:
                continue

            lo = max(0, idx - 700)
            hi = min(len(source), idx + len(token) + 700)
            candidates.append(source[lo:hi])

    return extract_publish_date_from_text(*candidates)


def extract_publish_date_from_row_context(
    *, html: str, href: str, link_text: str
) -> str | None:
    """Try extracting date from the same HTML table row as the target href.

    This avoids leaking dates from neighboring rows on dense publication tables.
    """
    source = html or ""
    source_l = source.lower()

    href_token = (href or "").strip()
    token_variants: list[str] = [href_token, unquote(href_token)]

    path = (urlparse(href_token).path or "").strip()
    if path:
        token_variants.extend([path, unquote(path)])
        base = path.rsplit("/", 1)[-1]
        if base:
            token_variants.extend([base, unquote(base)])

    seen_variants: set[str] = set()
    for raw_token in token_variants:
        token = (raw_token or "").strip()
        if not token:
            continue
        token_l = token.lower()
        if token_l in seen_variants:
            continue
        seen_variants.add(token_l)

        idx = source_l.find(token_l)
        if idx < 0:
            continue

        tr_start = source_l.rfind("<tr", 0, idx)
        tr_end = source_l.find("</tr>", idx)
        if tr_start >= 0 and tr_end > tr_start:
            row_html = source[tr_start : tr_end + len("</tr>")]
            row_html_l = row_html.lower()
            row_token_idx = row_html_l.find(token_l)
            if row_token_idx < 0:
                row_token_idx = 0
            lo = max(0, row_token_idx - 140)
            hi = min(len(row_html), row_token_idx + len(token) + 140)
            row_local = row_html[lo:hi]

            parsed = extract_publish_date_from_text(link_text)
            if not parsed:
                parsed = extract_publish_date_from_text(
                    row_local, include_edition=False
                )
            if parsed:
                return parsed

    return extract_publish_date_from_text(link_text)
