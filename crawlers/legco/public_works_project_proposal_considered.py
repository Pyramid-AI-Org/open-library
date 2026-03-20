from __future__ import annotations

import random
from typing import Any

import requests

from crawlers.base import RunContext, UrlRecord, clean_text, get_with_retries, sleep_seconds
from crawlers.legco.legco_helpers import (
    iso_from_dot_date,
    iter_bilingual_pdf_documents,
    parse_run_year,
    response_json,
    term_from_year,
)


def _proposal_page_url(base_url: str, locale: str, year: int) -> str:
    return (
        f"{base_url}/{locale}/legco-business/committees/public-works-subcommittee.html"
        f"?{year}#public-works-project-proposals-considered"
    )


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
        return {
            "en": clean_text(" ; ".join(en_parts)),
            "tc": clean_text(" ; ".join(tc_parts)),
        }

    text = str(value).strip() if value is not None else ""
    return {"en": text, "tc": text}


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


def _collect_project_codes(value: Any) -> str:
    if isinstance(value, dict):
        return clean_text(str(value.get("project_code", "")).strip())

    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            code = _collect_project_codes(item)
            if code:
                parts.append(code)
        return clean_text(" ; ".join(parts))

    return ""


def _first_iso_from_decision(value: Any) -> str | None:
    if isinstance(value, dict):
        return iso_from_dot_date(str(value.get("date", "")))

    if isinstance(value, list):
        for item in value:
            if not isinstance(item, dict):
                continue
            date_iso = iso_from_dot_date(str(item.get("date", "")))
            if date_iso:
                return date_iso

    return None


def _approved_iso_from_fc_papers(fc_papers: Any) -> str | None:
    if not isinstance(fc_papers, list):
        return None

    for paper in fc_papers:
        if not isinstance(paper, dict):
            continue
        approved_iso = _first_iso_from_decision(paper.get("decision"))
        if approved_iso:
            return approved_iso

    return None


class Crawler:
    """Crawl LegCo Public Works project proposals considered PWSC PDFs.

    Config: crawlers.legco.pages.public_works_project_proposal_considered
      - committee_slug: fc-subcommittee slug in data endpoint (default: public-works)
      - years_back: Number of years to include counting current year (default: 5)
    """

    name = "public_works_project_proposal_considered"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        base_url = str(cfg.get("base_url", "https://www.legco.gov.hk")).rstrip("/")
        committee_slug = str(cfg.get("committee_slug", "public-works")).strip() or "public-works"
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
                f"{base_url}/bi/data/committees/fc-subcommittees/{committee_slug}/"
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

                    subcom_paper = item.get("subcom_paper")
                    if not isinstance(subcom_paper, dict):
                        continue

                    endorsed_iso = _first_iso_from_decision(subcom_paper.get("decision"))
                    approved_iso = _approved_iso_from_fc_papers(item.get("fc_paper"))
                    if not approved_iso:
                        continue

                    approved_year = int(approved_iso[:4])
                    if approved_year not in target_years:
                        continue

                    pdf_obj = subcom_paper.get("url")
                    if not isinstance(pdf_obj, dict):
                        continue

                    subject_obj = subcom_paper.get("subject")
                    title_obj = _collect_text_by_locale(subject_obj)
                    subject_project_code = _collect_project_codes(subject_obj)
                    paper_no = clean_text(str(subcom_paper.get("paper_no", "")).strip())

                    docs = iter_bilingual_pdf_documents(
                        base_url=base_url,
                        pdf_obj=pdf_obj,
                        title_obj=title_obj,
                        seen_urls=seen_urls,
                    )
                    for abs_url, record_locale, paper_name in docs:
                        out.append(
                            ctx.make_record(
                                url=abs_url,
                                name=paper_name,
                                discovered_at_utc=ctx.started_at_utc,
                                publish_date=approved_iso,
                                source=self.name,
                                meta={
                                    "paper_no": paper_no,
                                    "endorsed_date": endorsed_iso,
                                    "approved_date": approved_iso,
                                    "subject": subject_project_code,
                                    "discovered_from": _proposal_page_url(
                                        base_url,
                                        record_locale,
                                        year,
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