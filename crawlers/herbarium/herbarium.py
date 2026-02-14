from __future__ import annotations

import random
import json
from datetime import datetime, timezone
from typing import Any

import requests

from crawlers.base import RunContext, UrlRecord, get_with_retries, sleep_seconds


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    params: list[tuple[str, str]] | None,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> requests.Response:
    return get_with_retries(
        session,
        url,
        params=params,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        backoff_jitter_seconds=backoff_jitter_seconds,
    )


_sleep_seconds = sleep_seconds


def _first_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list) and value:
        v0 = value[0]
        if isinstance(v0, str):
            return v0
    return ""


def _clean_spaces(s: str) -> str:
    return " ".join((s or "").strip().split())


def _filtered_meta(rec: dict[str, Any]) -> dict[str, Any]:
    drop_keys = {
        "photo_multimedia_type",
        "filepath_thumbnail",
        "filepath",
        "scientific_name_with_format",
        "scientific_name_with_authority_format",
        "photo",
    }

    out: dict[str, Any] = {}
    for k, v in rec.items():
        if k in drop_keys:
            continue
        out[k] = v
    return out


class Crawler:
    """Hong Kong Herbarium Plant Database crawler.

    Calls the public GetSpeciesList.php JSON endpoint and emits one output row per
    species_id (deduplicated by species_id only).

    Output mapping:
      - url: plant detail page for species_id
      - name: scientific_name_with_authority (best-effort)
      - meta: the returned record with a few large/irrelevant fields removed
      - source: "herbarium"
    """

    name = "herbarium"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})
        api_url = str(
            cfg.get(
                "api_url", "https://www.herbarium.gov.hk/plantdb/GetSpeciesList.php"
            )
        )

        plant_types = cfg.get("plant_type", []) or []
        if not isinstance(plant_types, list):
            plant_types = [plant_types]
        plant_types = [str(v) for v in plant_types if str(v).strip()]

        taxon_ranks = cfg.get("taxon_rank", []) or []
        if not isinstance(taxon_ranks, list):
            taxon_ranks = [taxon_ranks]
        taxon_ranks = [str(v) for v in taxon_ranks if str(v).strip()]

        order_by = str(cfg.get("order_by", "family")).strip() or "family"
        page_size = int(cfg.get("page_size", 4000))

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.10))
        max_total_records = int(cfg.get("max_total_records", 200000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 60))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})
        session.headers.setdefault("Accept", "application/json")

        discovered_at = ctx.started_at_utc or datetime.now(timezone.utc).isoformat()

        out: list[UrlRecord] = []
        seen_species_ids: set[str] = set()

        expected_total: int | None = None
        page_no = 1

        while True:
            params: list[tuple[str, str]] = []
            for pt in plant_types:
                params.append(("plant_type[]", pt))
            for tr in taxon_ranks:
                params.append(("taxon_rank[]", tr))

            params.extend(
                [
                    ("order_by", order_by),
                    ("page_size", str(page_size)),
                    ("page_no", str(page_no)),
                ]
            )

            if ctx.debug:
                print(f"[{self.name}] Fetch page_no={page_no} page_size={page_size}")

            resp = _get_with_retries(
                session,
                api_url,
                params=params,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )

            # The endpoint may include a UTF-8 BOM, which breaks requests' resp.json().
            payload_text = resp.content.decode("utf-8-sig", errors="replace")
            payload = json.loads(payload_text)
            if not isinstance(payload, dict):
                raise ValueError("Herbarium API returned non-object JSON")

            if expected_total is None:
                rc = payload.get("record_count")
                if rc is not None:
                    try:
                        expected_total = int(str(rc))
                    except Exception:
                        expected_total = None

            records = payload.get("records")
            if not isinstance(records, list) or not records:
                break

            new_on_page = 0

            for rec in records:
                if not isinstance(rec, dict):
                    continue

                species_id = str(rec.get("species_id") or "").strip()
                if not species_id:
                    continue

                if species_id in seen_species_ids:
                    continue
                seen_species_ids.add(species_id)
                new_on_page += 1

                meta = _filtered_meta(rec)

                name = _first_str(rec.get("scientific_name_with_authority"))
                name = _clean_spaces(name) or None

                url = (
                    "https://www.herbarium.gov.hk/en/hk-plant-database/plant-detail/index.html"
                    f"?pType=species&oID={species_id}"
                )

                out.append(
                    UrlRecord(
                        url=url,
                        name=name,
                        discovered_at_utc=discovered_at,
                        source=self.name,
                        meta=meta,
                    )
                )

                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

            # If the API starts repeating pages, avoid an infinite loop.
            if new_on_page == 0:
                break

            # Stop when we have everything (best-effort)
            if expected_total is not None and len(seen_species_ids) >= expected_total:
                break

            # Polite pacing between API pages
            delay = request_delay_seconds
            if request_jitter_seconds > 0:
                delay += random.uniform(0.0, request_jitter_seconds)
            _sleep_seconds(delay)

            page_no += 1

        out.sort(key=lambda r: r.url)
        return out
