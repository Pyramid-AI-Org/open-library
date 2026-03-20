from __future__ import annotations

import random
from typing import Any
import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    get_with_retries,
    sleep_seconds,
)
from crawlers.legco.legco_helpers import (
    iso_from_dot_date,
    iter_bilingual_pdf_documents,
    parse_run_year,
    response_json,
    term_from_year,
)


def _collect_cat_ids(node: Any, out: set[str]) -> None:
    if isinstance(node, list):
        for item in node:
            _collect_cat_ids(item, out)
        return

    if not isinstance(node, dict):
        return

    cat_id = str(node.get("cat_id", "")).strip()
    if cat_id:
        out.add(cat_id)

    for val in node.values():
        if isinstance(val, (dict, list)):
            _collect_cat_ids(val, out)


def _iter_nodes_with_pdf(node: Any):
    stack = [node]
    while stack:
        cur = stack.pop()

        if isinstance(cur, list):
            stack.extend(cur)
            continue

        if not isinstance(cur, dict):
            continue

        if isinstance(cur.get("pdf"), dict):
            yield cur

        for val in cur.values():
            if isinstance(val, (dict, list)):
                stack.append(val)


def _publish_iso_from_date_field(value: Any) -> str | None:
    if isinstance(value, str):
        return iso_from_dot_date(value)

    if not isinstance(value, list):
        return None

    for item in value:
        if isinstance(item, dict):
            publish_iso = iso_from_dot_date(str(item.get("value", "")))
        else:
            publish_iso = iso_from_dot_date(str(item))

        if publish_iso:
            return publish_iso

    return None


def _papers_page_url(base_url: str, locale: str, year: int, meeting_token: str) -> str:
    return (
        f"{base_url}/{locale}/legco-business/committees/meeting-papers.html"
        f"?fc&{year}&{meeting_token}"
    )


class Crawler:
    """Crawl LegCo Finance Committee papers-and-reports PDF links only.

    Config: crawlers.legco.pages.finance_papers_reports
      - years_back: Number of years to include counting current year (default: 5)
    """

    name = "finance_papers_reports"

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
        terms = sorted({term_from_year(year) for year in years}, reverse=True)

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

        for term in terms:
            papers_index_url = (
                f"{base_url}/bi/data/committees/finance-committee/"
                f"term-{term}/papers/papers.json"
            )

            if ctx.debug:
                print(f"[{self.name}] Fetch papers index -> {papers_index_url}")

            maybe_sleep()

            try:
                papers_index_resp = get_with_retries(
                    session,
                    papers_index_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                papers_index_data = response_json(papers_index_resp)
            except Exception as e:
                if ctx.debug:
                    print(f"[{self.name}] Skip term papers index term-{term}: {e}")
                continue

            cat_ids: set[str] = set()
            _collect_cat_ids(papers_index_data.get("paper"), cat_ids)

            for cat_id in sorted(cat_ids):
                cat_papers_url = (
                    f"{base_url}/bi/data/committees/finance-committee/"
                    f"term-{term}/papers/{cat_id}.json"
                )

                maybe_sleep()

                try:
                    cat_papers_resp = get_with_retries(
                        session,
                        cat_papers_url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_jitter_seconds=backoff_jitter_seconds,
                    )
                    cat_papers_data = response_json(cat_papers_resp)
                except Exception as e:
                    if ctx.debug:
                        print(f"[{self.name}] Skip papers category {cat_id}: {e}")
                    continue

                for item in _iter_nodes_with_pdf(cat_papers_data):
                    pdf_obj = item.get("pdf")
                    if not isinstance(pdf_obj, dict):
                        continue

                    title_obj = item.get("title")
                    if not isinstance(title_obj, dict):
                        title_obj = {}

                    publish_date = _publish_iso_from_date_field(item.get("date"))
                    if not publish_date:
                        continue

                    publish_year = int(publish_date[:4])
                    if publish_year not in target_years:
                        continue

                    meeting_token = publish_date.replace("-", "")

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
                                    "discovered_from": _papers_page_url(
                                        base_url,
                                        record_locale,
                                        publish_year,
                                        meeting_token,
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
