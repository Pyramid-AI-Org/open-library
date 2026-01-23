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


def _load_crawler_module(name: str):
    return importlib.import_module(f"crawlers.{name}")


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
    ap.add_argument("--crawler", default="", help="Run a specific crawler module name")
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
        crawler_names = ["example", "link_extract", "hksar_press_releases"]

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
