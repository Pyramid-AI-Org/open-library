from __future__ import annotations

import random

import requests

from crawlers.base import RunContext, UrlRecord, clean_text, get_with_retries, sleep_seconds
from crawlers.legco.legco_helpers import (
    extract_meeting_token,
    iso_from_dot_date,
    iter_bilingual_pdf_documents,
    parse_run_year,
    response_json,
    term_from_year,
)


def _papers_reports_page_url(base_url: str, locale: str, year: int) -> str:
    return (
        f"{base_url}/{locale}/legco-business/committees/public-works-subcommittee.html"
        f"?{year}#papers-and-reports"
    )


class Crawler:
    """Crawl LegCo Public Works Subcommittee papers-and-reports PDF links.

    Config: crawlers.legco.pages.public_works_papers_reports
      - committee_slug: fc-subcommittee slug in data endpoint (default: public-works)
      - years_back: Number of years to include counting current year (default: 5)
    """

    name = "public_works_papers_reports"

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
            meetings_json_url = (
                f"{base_url}/bi/data/committees/fc-subcommittees/{committee_slug}/"
                f"term-{term}/{year}/meetings.json"
            )

            if ctx.debug:
                print(f"[{self.name}] Fetch meetings -> {meetings_json_url}")

            maybe_sleep()

            try:
                meetings_resp = get_with_retries(
                    session,
                    meetings_json_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                meetings_data = response_json(meetings_resp)
            except Exception as e:
                if ctx.debug:
                    print(f"[{self.name}] Skip year {year}: {e}")
                continue

            schedule = meetings_data.get("schedule")
            if not isinstance(schedule, list):
                continue

            for row in schedule:
                if not isinstance(row, dict):
                    continue

                papers_flag = str(row.get("papers", "")).strip().upper()
                if papers_flag != "Y":
                    continue

                dates = row.get("dates")
                date_token = dates[0] if isinstance(dates, list) and dates else ""
                publish_date = iso_from_dot_date(str(date_token))

                meeting_token = extract_meeting_token(row, publish_date)
                if not meeting_token:
                    continue

                papers_json_url = (
                    f"{base_url}/bi/data/committees/fc-subcommittees/{committee_slug}/"
                    f"term-{term}/{year}/papers/{meeting_token}.json"
                )

                if ctx.debug:
                    print(f"[{self.name}] Fetch papers -> {papers_json_url}")

                maybe_sleep()

                try:
                    papers_resp = get_with_retries(
                        session,
                        papers_json_url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_jitter_seconds=backoff_jitter_seconds,
                    )
                    papers_data = response_json(papers_resp)
                except Exception as e:
                    if ctx.debug:
                        print(f"[{self.name}] Skip papers {meeting_token}: {e}")
                    continue

                papers = papers_data.get("papers")
                if not isinstance(papers, list):
                    continue

                for paper in papers:
                    if not isinstance(paper, dict):
                        continue

                    pdf_obj = paper.get("pdf")
                    if not isinstance(pdf_obj, dict):
                        continue

                    title_obj = paper.get("title")
                    if not isinstance(title_obj, dict):
                        title_obj = {}

                    paper_no = clean_text(str(paper.get("paper_no", "")).strip())

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
                                publish_date=publish_date,
                                source=self.name,
                                meta={
                                    "paper_no": paper_no,
                                    "discovered_from": _papers_reports_page_url(
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
