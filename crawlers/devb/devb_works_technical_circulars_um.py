from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

from crawlers.base import RunContext, UrlRecord


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


def _parse_run_year(run_date_utc: str) -> int:
    # Expected format: YYYY-MM-DD
    return date.fromisoformat(run_date_utc).year


def _parse_ddmmyyyy_to_iso(value: str | None) -> str | None:
    s = (value or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None


def _path_ext(url: str) -> str:
    p = urlparse(url)
    path = (p.path or "").lower()
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


_LIST_RE = re.compile(r"var\s+list\s*=\s*(\[[\s\S]*?\])\s*(?:;|%|$)")


@dataclass(frozen=True)
class _CircularFile:
    url: str
    circular_number: str | None
    title: str | None
    index_groups: list[str]
    issue_year: str | None
    issue_date_iso: str | None
    revision_year: str | None
    revision_date_iso: str | None


def _extract_active_list(js_text: str) -> list[dict[str, Any]]:
    m = _LIST_RE.search(js_text or "")
    if not m:
        raise ValueError("Could not locate 'var list = [...]' in JS payload")

    payload = m.group(1)
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse list JSON: {e}") from e

    if not isinstance(data, list):
        raise ValueError("Parsed list payload is not a JSON array")

    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            out.append(item)
    return out


class Crawler:
    """Crawl DevB Works Technical Circulars (in-force list) via JS dataset.

    The page at:
      https://www.devb.gov.hk/en/publications_and_press_releases/technical_circulars/technical_circulars_um/index.html

    populates its results table client-side from:
      https://www.devb.gov.hk/filemanager/technicalcirculars/list_technicalcirculars_53.js

    This crawler downloads the JS dataset, parses the active `list` array, and then
    emulates the UI's Issue Year filter for the last N years. The UI includes rows
    that match either IssueYear or RevisionYear.

    Emits one UrlRecord per file URL, with circular metadata in `meta`.

    Config: crawlers.devb_works_technical_circulars_um
      - base_url: https://www.devb.gov.hk
      - data_js_url: (optional override)
      - years_back: 10
      - include_revision_year_matches: true
      - request_delay_seconds: 0.0
      - request_jitter_seconds: 0.0
      - max_total_records: 50000
      - backoff_base_seconds: 0.5
      - backoff_jitter_seconds: 0.25
    """

    name = "devb_works_technical_circulars_um"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.devb.gov.hk")).rstrip("/")
        data_js_url = str(
            cfg.get(
                "data_js_url",
                f"{base_url}/filemanager/technicalcirculars/list_technicalcirculars_53.js",
            )
        ).strip()

        years_back = int(cfg.get("years_back", 10))
        include_revision_year_matches = bool(
            cfg.get("include_revision_year_matches", True)
        )
        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.0))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.0))
        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        run_year = _parse_run_year(ctx.run_date_utc)
        if years_back <= 0:
            return []
        years = [str(y) for y in range(run_year, run_year - years_back, -1)]

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if ctx.debug:
            print(f"[{self.name}] Fetch dataset -> {data_js_url}")

        resp = _get_with_retries(
            session,
            data_js_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        items = _extract_active_list(resp.text)

        # url -> (UrlRecord skeleton + matched years)
        by_url: dict[str, tuple[UrlRecord, set[str]]] = {}

        for year in years:
            # The site builds results client-side, but we keep a small per-year delay
            # to be consistent/polite if this ever changes to server-side.
            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            for item in items:
                issue_year = item.get("IssueYear") or ""
                revision_year = item.get("RevisionYear") or ""

                matches_year = False
                if issue_year == year:
                    matches_year = True
                elif include_revision_year_matches and revision_year == year:
                    matches_year = True

                if not matches_year:
                    continue

                circular_number = item.get("CircularNumber") or None
                title = item.get("Title") or None

                index_groups_raw = item.get("IndexGroup")
                index_groups: list[str] = []
                if isinstance(index_groups_raw, list):
                    for v in index_groups_raw:
                        if v is None:
                            continue
                        s = str(v).strip()
                        if s:
                            index_groups.append(s)

                files_raw = item.get("Files")
                if not isinstance(files_raw, list) or not files_raw:
                    continue

                issue_date_iso = _parse_ddmmyyyy_to_iso(item.get("IssueDate"))
                revision_date_iso = _parse_ddmmyyyy_to_iso(item.get("RevisionDate"))
                chosen_date_iso = issue_date_iso or revision_date_iso

                for file_path in files_raw:
                    if file_path is None:
                        continue
                    file_path_s = str(file_path).strip()
                    if not file_path_s:
                        continue

                    abs_url = urljoin(base_url + "/", file_path_s.lstrip("/"))
                    if not abs_url.startswith(base_url + "/"):
                        continue

                    # Policy: ignore spreadsheets and Word docs.
                    if _path_ext(abs_url) in (".xls", ".xlsx", ".doc", ".docx"):
                        continue

                    # Keep human title in `name` (viewer uses `name` as the main label).
                    # Fall back to circular number if title is missing.
                    name = title or circular_number or None

                    record = UrlRecord(
                        url=abs_url,
                        name=name,
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "circular_no": circular_number,
                            "index_groups": index_groups,
                            "date": chosen_date_iso,
                            "issue_year": issue_year or None,
                            "issue_date": issue_date_iso,
                            "revision_year": revision_year or None,
                            "revision_date": revision_date_iso,
                            "matched_years": [year],
                            "data_js_url": data_js_url,
                        },
                    )

                    prev = by_url.get(abs_url)
                    if prev is None:
                        by_url[abs_url] = (record, {year})
                    else:
                        # Keep the first record object, but merge year matches.
                        prev_record, prev_years = prev
                        prev_years.add(year)
                        by_url[abs_url] = (prev_record, prev_years)

                    if len(by_url) >= max_total_records:
                        break
                if len(by_url) >= max_total_records:
                    break
            if len(by_url) >= max_total_records:
                break

        out: list[UrlRecord] = []
        for rec, years_set in by_url.values():
            # Patch matched years (sorted) into meta.
            meta = dict(rec.meta)
            meta["matched_years"] = sorted(years_set, reverse=True)
            out.append(
                UrlRecord(
                    url=rec.url,
                    name=rec.name,
                    discovered_at_utc=rec.discovered_at_utc,
                    source=rec.source,
                    meta=meta,
                )
            )

        out.sort(key=lambda r: (r.url or ""))
        return out
