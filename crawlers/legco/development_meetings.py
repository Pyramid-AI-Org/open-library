from __future__ import annotations

import random
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)
from crawlers.legco.legco_helpers import (
    build_meeting_name,
    build_minutes_fallback_url,
    canonicalize_legco_url,
    display_from_iso_date_localized,
    extract_meeting_token,
    extract_panel_code,
    iso_from_dot_date,
    locale_from_url,
    parse_run_year,
    pick_title_for_pdf,
    response_json,
    term_from_year,
    url_exists,
)

_ALLOWED_DOC_EXTS = {".pdf"}


class Crawler:
    """Crawl LegCo Panel on Development meetings and papers PDF links.

    Config: crawlers.legco.pages.development_meetings
      - panel_slug: Panel slug in panel URL and JSON endpoints (default: development)
      - years_back: Number of years to include counting current year (default: 5)
    """

    name = "development_meetings"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        base_url = str(cfg.get("base_url", "https://www.legco.gov.hk")).rstrip("/")
        panel_slug = str(cfg.get("panel_slug", "development")).strip() or "development"

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

        for year in years:
            term = term_from_year(year)

            panel_page_url_by_lang = {
                "en": (
                    f"{base_url}/en/legco-business/committees/panel.html?{panel_slug}&{year}#meetings"
                ),
                "tc": (
                    f"{base_url}/tc/legco-business/committees/panel.html?{panel_slug}&{year}#meetings"
                ),
            }
            meetings_json_url = (
                f"{base_url}/bi/data/committees/panels/{panel_slug}/"
                f"term-{term}/{year}/meetings.json"
            )

            if ctx.debug:
                print(f"[{self.name}] Fetch meetings -> {meetings_json_url}")

            if request_delay_seconds > 0:
                sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            try:
                resp = get_with_retries(
                    session,
                    meetings_json_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
                meetings_data = response_json(resp)
            except Exception as e:
                if ctx.debug:
                    print(f"[{self.name}] Skip year {year}: {e}")
                continue

            schedule = meetings_data.get("schedule")
            if not isinstance(schedule, list):
                continue

            committee_file_path = meetings_data.get("committee_file_path")
            if not isinstance(committee_file_path, dict):
                committee_file_path = {}

            panel_code = extract_panel_code(str(committee_file_path.get("en", "")))

            for row in schedule:
                if not isinstance(row, dict):
                    continue

                dates = row.get("dates")
                date_token = dates[0] if isinstance(dates, list) and dates else ""
                publish_date = iso_from_dot_date(str(date_token))
                date_compact = publish_date.replace("-", "") if publish_date else ""

                minutes_obj = row.get("minutes")
                if not isinstance(minutes_obj, dict):
                    minutes_obj = {}

                for lang_key in ("en", "tc"):
                    href = str(minutes_obj.get(lang_key, "")).strip()

                    if not href and panel_code and date_compact:
                        path_for_lang = str(
                            committee_file_path.get(lang_key, "")
                        ).strip()
                        if path_for_lang:
                            fallback_abs = build_minutes_fallback_url(
                                base_url=base_url,
                                committee_file_path=path_for_lang,
                                panel_code=panel_code,
                                meeting_token=date_compact,
                            )
                            if url_exists(
                                session,
                                fallback_abs,
                                timeout_seconds=timeout_seconds,
                            ):
                                href = fallback_abs

                    if not href:
                        continue

                    abs_url = canonicalize_legco_url(urljoin(base_url + "/", href))
                    if not abs_url:
                        continue
                    record_locale = locale_from_url(abs_url, fallback=lang_key)
                    if path_ext(abs_url) not in _ALLOWED_DOC_EXTS:
                        continue
                    if abs_url in seen_urls:
                        continue

                    seen_urls.add(abs_url)
                    date_display = display_from_iso_date_localized(
                        publish_date, record_locale
                    ) or str(date_token)
                    out.append(
                        ctx.make_record(
                            url=abs_url,
                            name=build_meeting_name(
                                "Panel on Development",
                                "發展事務委員會",
                                record_locale,
                                date_display,
                            ),
                            discovered_at_utc=ctx.started_at_utc,
                            publish_date=publish_date,
                            source=self.name,
                            meta={
                                "discovered_from": panel_page_url_by_lang.get(
                                    record_locale, panel_page_url_by_lang["en"]
                                ),
                                "locale": record_locale,
                            },
                        )
                    )

                    if len(out) >= max_total_records:
                        out.sort(key=lambda r: r.url)
                        return out

                papers_flag = str(row.get("papers", "")).strip().upper()
                if papers_flag != "Y":
                    continue

                meeting_token = extract_meeting_token(row, publish_date)
                if not meeting_token:
                    continue

                papers_page_url_by_lang = {
                    "en": (
                        f"{base_url}/en/legco-business/committees/meeting-papers.html"
                        f"?panels={panel_slug}&{year}&{meeting_token}"
                    ),
                    "tc": (
                        f"{base_url}/tc/legco-business/committees/meeting-papers.html"
                        f"?panels={panel_slug}&{year}&{meeting_token}"
                    ),
                }
                papers_json_url = (
                    f"{base_url}/bi/data/committees/panels/{panel_slug}/"
                    f"term-{term}/{year}/papers/{meeting_token}.json"
                )

                if request_delay_seconds > 0:
                    sleep_seconds(
                        request_delay_seconds
                        + random.uniform(0.0, request_jitter_seconds)
                    )

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

                    for lang_key in ("en", "tc"):
                        pdf_href = str(pdf_obj.get(lang_key, "")).strip()
                        if not pdf_href:
                            continue

                        abs_url = canonicalize_legco_url(
                            urljoin(base_url + "/", pdf_href)
                        )
                        if not abs_url:
                            continue
                        record_locale = locale_from_url(abs_url, fallback=lang_key)
                        if path_ext(abs_url) not in _ALLOWED_DOC_EXTS:
                            continue
                        if abs_url in seen_urls:
                            continue

                        seen_urls.add(abs_url)

                        paper_name = pick_title_for_pdf(abs_url, title_obj)
                        if not paper_name:
                            paper_name = infer_name_from_link(None, abs_url)

                        out.append(
                            ctx.make_record(
                                url=abs_url,
                                name=paper_name,
                                discovered_at_utc=ctx.started_at_utc,
                                publish_date=publish_date,
                                source=self.name,
                                meta={
                                    "discovered_from": papers_page_url_by_lang.get(
                                        record_locale, papers_page_url_by_lang["en"]
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
