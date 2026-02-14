from __future__ import annotations

import argparse
import importlib
from datetime import timezone
from pathlib import Path
from typing import Any

from crawlers.base import RunContext
from utils.jsonio import sha256_file, write_json, write_jsonl
from utils.settings import load_settings
from utils.time import utc_date_yyyymmdd, utc_now


_CRAWLER_MODULE_ALIASES: dict[str, str] = {
    # Backward-compatible aliases for old flat module names.
    "hksar_press_releases": "hksar.hksar_press_releases",
    "devb_press_releases": "devb.devb_press_releases",
    "devb_speeches_and_presentations": "devb.devb_speeches_and_presentations",
    "devb_general_circulars": "devb.devb_general_circulars",
    "devb_planning_and_lands_technical_circulars": "devb.devb_planning_and_lands_technical_circulars",
    "devb_works_technical_circulars_um": "devb.devb_works_technical_circulars_um",
    "devb_publications": "devb.devb_publications",
    "devb_construction_site_safety_manual": "devb.devb_construction_site_safety_manual",
    "devb_standard_consultancy_documents": "devb.devb_standard_consultancy_documents",
    "devb_standard_contract_documents": "devb.devb_standard_contract_documents",
    "codes_design_manuals_and_guidelines": "bd.codes_design_manuals_and_guidelines",
    "practice_notes_and_circular_letters": "bd.practice_notes_and_circular_letters",
    "central_data_bank": "bd.central_data_bank",
    "bd_basic_pages": "bd.basic_pages",
    "scheduled_areas": "bd.scheduled_areas",
    "notices_and_reports": "bd.notices_and_reports",
    "archsd_technical_documents": "archsd.archsd_technical_documents",
    "archsd_practices_and_guidelines": "archsd.archsd_practices_and_guidelines",
    "cedd_technical_circulars": "cedd.cedd_technical_circulars",
    "cedd_geo_publications": "cedd.cedd_geo_publications",
    "cedd_eacsb_handbook": "cedd.cedd_eacsb_handbook",
    "cedd_standards_spec_handbooks_cost": "cedd.cedd_standards_spec_handbooks_cost",
    "cedd_ceo_publications": "cedd.cedd_ceo_publications",
    "tel_directory": "directory.tel_directory",
    "herbarium": "herbarium.herbarium",
}


def _load_crawler_module(name: str):
    raw = name.strip()
    if not raw:
        raise ValueError("crawler name is empty")

    normalized = raw.replace("/", ".")
    if normalized.startswith("crawlers."):
        return importlib.import_module(normalized)

    normalized = _CRAWLER_MODULE_ALIASES.get(normalized, normalized)
    return importlib.import_module(f"crawlers.{normalized}")


def _run_one(crawler_name: str, ctx: RunContext) -> list[dict[str, Any]]:
    mod = _load_crawler_module(crawler_name)
    crawler = mod.Crawler()
    records = crawler.crawl(ctx)

    out: list[dict[str, Any]] = []
    for r in records:
        out.append(
            {
                "url": r.url,
                "name": r.name,
                "discovered_at_utc": r.discovered_at_utc,
                "source": r.source,
                "meta": r.meta,
            }
        )

    out.sort(key=lambda rec: (rec.get("url") or ""))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Run web crawlers and write results")
    ap.add_argument(
        "--crawler",
        default="",
        help=(
            "Run a specific crawler module path relative to crawlers "
            "(e.g. devb.devb_press_releases)."
        ),
    )
    ap.add_argument("--settings", default="config/settings.yaml")
    ap.add_argument(
        "--out", default="data", help="Output root (usually points to data worktree)"
    )
    ap.add_argument("--debug", action="store_true")
    ap.add_argument(
        "--run-date", default="", help="UTC date YYYY-MM-DD (defaults to today)"
    )

    args = ap.parse_args()

    settings = load_settings(args.settings)

    now = utc_now()
    run_date = args.run_date.strip() or utc_date_yyyymmdd(now)

    ctx = RunContext(
        run_date_utc=run_date,
        started_at_utc=now.astimezone(timezone.utc).isoformat(),
        settings=settings,
        debug=bool(args.debug),
    )

    crawler_names: list[str]
    if args.crawler.strip():
        crawler_names = [args.crawler.strip()]
    else:
        # Keep this explicit so adding crawlers is intentional/reviewable.
        crawler_names = [
            "hksar.hksar_press_releases",
            "devb.devb_press_releases",
            "devb.devb_speeches_and_presentations",
            "devb.devb_general_circulars",
            "devb.devb_planning_and_lands_technical_circulars",
            "devb.devb_works_technical_circulars_um",
            "devb.devb_publications",
            "bd.codes_design_manuals_and_guidelines",
            "bd.practice_notes_and_circular_letters",
            "bd.central_data_bank",
            "bd.basic_pages",
            "bd.scheduled_areas",
            "bd.notices_and_reports",
            "archsd.archsd_technical_documents",
            "archsd.archsd_practices_and_guidelines",
            "cedd.cedd_technical_circulars",
            "cedd.cedd_geo_publications",
            "cedd.cedd_eacsb_handbook",
            "cedd.cedd_standards_spec_handbooks_cost",
            "cedd.cedd_ceo_publications",
            "directory.tel_directory",
            "herbarium.herbarium",
        ]

    all_records: list[dict[str, Any]] = []
    for name in crawler_names:
        all_records.extend(_run_one(name, ctx))

    # Deterministic ordering & basic de-dup by url+source
    all_records.sort(key=lambda r: (r.get("url") or "", r.get("source") or ""))

    out_root = Path(args.out)

    latest_dir = out_root / "latest"
    urls_path = latest_dir / "urls.jsonl"
    rows = write_jsonl(urls_path, all_records)

    summary = {
        "run_date_utc": run_date,
        "started_at_utc": ctx.started_at_utc,
        "crawler": args.crawler.strip() or "all",
        "rows": rows,
    }
    write_json(latest_dir / "summary.json", summary)

    manifest = {
        "run_date_utc": run_date,
        "generated_at_utc": utc_now().isoformat(),
        "schema_version": 1,
        "outputs": [
            {
                "path": str(urls_path.as_posix()),
                "rows": rows,
                "sha256": sha256_file(urls_path),
                "bytes": urls_path.stat().st_size,
            },
            {
                "path": str((latest_dir / "summary.json").as_posix()),
                "sha256": sha256_file(latest_dir / "summary.json"),
                "bytes": (latest_dir / "summary.json").stat().st_size,
            },
        ],
    }
    write_json(latest_dir / "manifest.json", manifest)

    print(f"Wrote {rows} rows to {urls_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
