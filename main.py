from __future__ import annotations

import argparse
import importlib
import json
from datetime import timezone
from pathlib import Path
from typing import Any

from crawlers.base import RunContext
from utils.jsonio import iter_jsonl, sha256_file, write_json, write_jsonl
from utils.settings import load_settings
from utils.time import utc_now


def _get_source_label(settings: dict[str, Any], source_id: str) -> str:
    """Get the label for a source from settings."""
    crawlers_cfg = settings.get("crawlers", {})
    source_cfg = crawlers_cfg.get(source_id, {})
    return source_cfg.get("label", source_id)


def _get_all_crawlers_from_settings(
    settings: dict[str, Any],
) -> list[tuple[str, str, str]]:
    """
    Extract all crawler definitions from settings.

    Returns list of (source_id, crawler_name, module_path) tuples.

    Settings structure:
      crawlers:
        devb:
          label: "The Development Bureau"
          pages:
            devb_press_releases:
              ...
            devb_speeches_and_presentations:
              ...
    """
    crawlers_cfg = settings.get("crawlers", {})
    result = []

    for source_id, source_cfg in crawlers_cfg.items():
        if not isinstance(source_cfg, dict):
            continue
        pages_cfg = source_cfg.get("pages", {})
        if not isinstance(pages_cfg, dict):
            continue

        for crawler_name in pages_cfg.keys():
            # Module path: crawlers.<source_id>.<crawler_name>
            module_path = f"{source_id}.{crawler_name}"
            result.append((source_id, crawler_name, module_path))

    return result


def _load_crawler_module(module_path: str):
    """Load a crawler module by its path (e.g., 'devb.devb_press_releases')."""
    raw = module_path.strip()
    if not raw:
        raise ValueError("crawler module path is empty")

    full_path = f"crawlers.{raw}"
    return importlib.import_module(full_path)


def _find_previous_urls_jsonl(out_root: Path) -> Path | None:
    latest_path = out_root / "latest" / "urls.jsonl"
    if latest_path.exists():
        return latest_path

    archive_root = out_root / "archive"
    if not archive_root.exists():
        return None

    candidates = sorted(archive_root.glob("*/*/*/urls.jsonl"))
    if not candidates:
        return None
    return candidates[-1]


def _record_key(rec: dict[str, Any]) -> tuple[str, str] | None:
    source = rec.get("source")
    url = rec.get("url")
    if not isinstance(source, str) or not isinstance(url, str):
        return None
    s = source.strip()
    u = url.strip()
    if not s or not u:
        return None
    return s, u


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(obj, dict):
        return obj
    return {}


def _load_v2_latest_records(out_root: Path) -> list[dict[str, Any]] | None:
    root = out_root / "archive_v2"
    if not root.exists():
        return None

    day_meta_paths = sorted(root.glob("*/*/days/*/meta.json"))
    if not day_meta_paths:
        return None

    latest_meta_path = day_meta_paths[-1]
    meta = _load_json_dict(latest_meta_path)
    base_path = out_root / str(meta.get("base_path") or "")
    added_path = out_root / str(meta.get("added_path") or "")
    removed_path = out_root / str(meta.get("removed_path") or "")
    if not base_path.exists() or not base_path.is_file():
        return None

    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in iter_jsonl(base_path):
        key = _record_key(rec)
        if key is None:
            continue
        by_key[key] = rec

    if removed_path.exists() and removed_path.is_file():
        for rec in iter_jsonl(removed_path):
            key = _record_key(rec)
            if key is None:
                continue
            by_key.pop(key, None)

    if added_path.exists() and added_path.is_file():
        for rec in iter_jsonl(added_path):
            key = _record_key(rec)
            if key is None:
                continue
            by_key[key] = rec

    return list(by_key.values())


def _load_previous_records_by_source(
    out_root: Path,
) -> dict[str, dict[str, dict[str, Any]]]:
    records = _load_v2_latest_records(out_root)
    if records is None:
        path = _find_previous_urls_jsonl(out_root)
        if path is None:
            return {}
        records = list(iter_jsonl(path))

    out: dict[str, dict[str, dict[str, Any]]] = {}
    for rec in records:
        source = rec.get("source")
        url = rec.get("url")
        if not isinstance(source, str) or not isinstance(url, str):
            continue
        s = source.strip()
        u = url.strip()
        if not s or not u:
            continue
        by_url = out.setdefault(s, {})
        if u not in by_url:
            by_url[u] = rec
    return out


def _run_one(
    source_id: str,
    crawler_name: str,
    module_path: str,
    settings: dict[str, Any],
    run_date: str,
    started_at: str,
    debug: bool,
    prior_records_by_url: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Run a single crawler and return its records."""
    source_label = _get_source_label(settings, source_id)

    ctx = RunContext(
        run_date_utc=run_date,
        started_at_utc=started_at,
        settings=settings,
        source_id=source_id,
        source_label=source_label,
        debug=debug,
        prior_records_by_url=prior_records_by_url,
    )

    mod = _load_crawler_module(module_path)
    crawler = mod.Crawler()
    records = crawler.crawl(ctx)

    out: list[dict[str, Any]] = []
    for r in records:
        out.append(
            {
                "url": r.url,
                "name": r.name,
                "discovered_at_utc": r.discovered_at_utc,
                "publish_date": r.publish_date,
                "source": r.source,
                "source_id": r.source_id,
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
            "Run a specific crawler by module path (e.g., devb.devb_press_releases) "
            "or by short name (e.g., devb_press_releases)."
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
    out_root = Path(args.out)
    previous_records_by_source = _load_previous_records_by_source(out_root)

    now = utc_now()
    if args.run_date.strip():
        # Parse provided date as YYYY-MM-DD and assume midnight UTC
        from datetime import datetime
        run_date = datetime.strptime(args.run_date.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()
    else:
        run_date = now.astimezone(timezone.utc).isoformat()
    started_at = now.astimezone(timezone.utc).isoformat()

    # Determine which crawlers to run
    all_crawlers = _get_all_crawlers_from_settings(settings)

    crawlers_to_run: list[
        tuple[str, str, str]
    ]  # (source_id, crawler_name, module_path)

    crawler_arg = args.crawler.strip()
    if crawler_arg:
        # Run specific crawler
        # Support both full path (devb.devb_press_releases) and short name (devb_press_releases)
        found = None
        for source_id, crawler_name, module_path in all_crawlers:
            if module_path == crawler_arg or crawler_name == crawler_arg:
                found = (source_id, crawler_name, module_path)
                break

        if not found:
            # Try to parse as source.crawler format
            if "." in crawler_arg:
                source_id, crawler_name = crawler_arg.split(".", 1)
                found = (source_id, crawler_name, crawler_arg)
            else:
                raise ValueError(f"Crawler not found: {crawler_arg}")

        crawlers_to_run = [found]
    else:
        # Run all crawlers
        crawlers_to_run = all_crawlers

    all_records: list[dict[str, Any]] = []
    for source_id, crawler_name, module_path in crawlers_to_run:
        try:
            records = _run_one(
                source_id,
                crawler_name,
                module_path,
                settings,
                run_date,
                started_at,
                bool(args.debug),
                prior_records_by_url=previous_records_by_source.get(crawler_name),
            )
            all_records.extend(records)
            print(f"  {module_path}: {len(records)} records")
        except Exception as e:
            print(f"  {module_path}: ERROR - {e}")
            if args.debug:
                raise

    # Deterministic ordering & basic de-dup by url+source
    all_records.sort(key=lambda r: (r.get("url") or "", r.get("source") or ""))

    latest_dir = out_root / "latest"
    urls_path = latest_dir / "urls.jsonl"
    rows = write_jsonl(urls_path, all_records)

    summary = {
        "run_date_utc": run_date,
        "started_at_utc": started_at,
        "crawler": crawler_arg or "all",
        "rows": rows,
    }
    write_json(latest_dir / "summary.json", summary)

    manifest = {
        "run_date_utc": run_date,
        "generated_at_utc": utc_now().isoformat(),
        "schema_version": 2,
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
