from __future__ import annotations

import random
from typing import Any

import requests

from crawlers.base import RunContext, UrlRecord, get_with_retries, sleep_seconds
from crawlers.legco.legco_helpers import (
    iso_from_dot_date,
    iter_bilingual_pdf_documents,
    parse_run_year,
    response_json,
    term_from_year,
)


def _proposal_page_url(base_url: str, locale: str, year: int) -> str:
    return (
        f"{base_url}/{locale}/legco-business/committees/finance-committee.html"
        f"?{year}#financial-proposals-considered"
    )


def _collect_url_objects(url_field: Any) -> list[dict[str, Any]]:
    if isinstance(url_field, dict):
        return [url_field]

    if isinstance(url_field, list):
        out: list[dict[str, Any]] = []
        for item in url_field:
            if isinstance(item, dict):
                out.append(item)
        return out

    return []


def _collect_text_by_locale(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {
            "en": str(value.get("en", "")).strip(),
            "tc": str(value.get("tc", "")).strip(),
        }

    if isinstance(value, list):
        en_parts: list[str] = []
        tc_parts: list[str] = []
        for item in value:
            data = _collect_text_by_locale(item)
            if data["en"]:
                en_parts.append(data["en"])
            if data["tc"]:
                tc_parts.append(data["tc"])
        return {"en": " ; ".join(en_parts), "tc": " ; ".join(tc_parts)}

    text = str(value).strip() if value is not None else ""
    return {"en": text, "tc": text}


def _normalize_paper_numbers(value: Any) -> str:
    if isinstance(value, list):
        parts = [str(v).strip() for v in value if str(v).strip()]
        return ", ".join(parts)

    return str(value).strip() if value is not None else ""


def _title_obj_from_entry(entry: dict[str, Any]) -> dict[str, str]:
    subject = _collect_text_by_locale(entry.get("subject"))
    return {
        "en": subject["en"],
        "tc": subject["tc"],
    }


def _meta_value_for_locale(value: Any, locale: str) -> str:
    localized = _collect_text_by_locale(value)
    preferred = localized.get(locale, "").strip()
    if preferred:
        return preferred

    if locale != "en":
        en_fallback = localized.get("en", "").strip()
        if en_fallback:
            return en_fallback

    return ""


def _normalized_commitment(value: Any, locale: str) -> str:
    commitment = _meta_value_for_locale(value, locale).strip()
    if not commitment:
        return "不適用" if locale == "tc" else "Not Applicable"

    lowered = commitment.lower()
    if lowered in {"not applicable", "n/a", "na"}:
        return "不適用" if locale == "tc" else "Not Applicable"

    return commitment


def _approved_iso(item: dict[str, Any]) -> str | None:
    decision = item.get("decision")
    if not isinstance(decision, dict):
        return None

    decision_date = str(decision.get("date", "")).strip()
    if not decision_date:
        return None

    return iso_from_dot_date(decision_date)


def _iter_paper_nodes(item: dict[str, Any]):
    main_paper = item.get("main_paper")
    if isinstance(main_paper, dict):
        yield main_paper

    sub_paper = item.get("sub_paper")
    if isinstance(sub_paper, list):
        for entry in sub_paper:
            if isinstance(entry, dict):
                yield entry


class Crawler:
    """Crawl LegCo Finance Committee Financial Proposals Considered PDFs.

    Config: crawlers.legco.pages.finance_financial_proposals_considered
      - years_back: Number of years to include counting current year (default: 5)
    """

    name = "finance_financial_proposals_considered"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        base_url = str(cfg.get("base_url", "https://www.legco.gov.hk")).rstrip("/")
        years_back = max(1, int(cfg.get("years_back", 5)))
        max_total_records = int(cfg.get("max_total_records", 50000))

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.5))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.25))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        run_year = parse_run_year(ctx.run_date_utc)
        years = [run_year - offset for offset in range(years_back)]
        target_years = set(years)

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        def maybe_sleep() -> None:
            if request_delay_seconds > 0:
                sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

        for year in years:
            term = term_from_year(year)
            proposals_json_url = (
                f"{base_url}/bi/data/committees/finance-committee/"
                f"term-{term}/{year}/proposals.json"
            )

            if ctx.debug:
                print(f"[{self.name}] Fetch proposals -> {proposals_json_url}")

            maybe_sleep()

            try:
                proposals_resp = get_with_retries(
                    session,
                    proposals_json_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                proposals_data = response_json(proposals_resp)
            except Exception as e:
                if ctx.debug:
                    print(f"[{self.name}] Skip year {year}: {e}")
                continue

            proposals = proposals_data.get("proposals")
            if not isinstance(proposals, list):
                continue

            for proposal_group in proposals:
                if not isinstance(proposal_group, dict):
                    continue

                items = proposal_group.get("list")
                if not isinstance(items, list):
                    continue

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    approved_date = _approved_iso(item)
                    if not approved_date:
                        continue

                    approved_year = int(approved_date[:4])
                    if approved_year not in target_years:
                        continue

                    for paper_node in _iter_paper_nodes(item):
                        title_obj = _title_obj_from_entry(paper_node)
                        paper_no = _normalize_paper_numbers(paper_node.get("paper_no"))

                        for url_obj in _collect_url_objects(paper_node.get("url")):
                            docs = iter_bilingual_pdf_documents(
                                base_url=base_url,
                                pdf_obj=url_obj,
                                title_obj=title_obj,
                                seen_urls=seen_urls,
                            )
                            for abs_url, record_locale, paper_name in docs:
                                out.append(
                                    ctx.make_record(
                                        url=abs_url,
                                        name=paper_name,
                                        discovered_at_utc=ctx.started_at_utc,
                                        publish_date=approved_date,
                                        source=self.name,
                                        meta={
                                            "approved_date": approved_date,
                                            "discovered_from": _proposal_page_url(
                                                base_url, record_locale, year
                                            ),
                                            "paper_no": paper_no,
                                            "commitment": _normalized_commitment(
                                                paper_node.get("commitment"),
                                                record_locale,
                                            ),
                                            "locale": record_locale,
                                        },
                                    )
                                )

                                if len(out) >= max_total_records:
                                    out.sort(key=lambda r: r.url)
                                    return out

        out.sort(key=lambda r: r.url)
        return out
