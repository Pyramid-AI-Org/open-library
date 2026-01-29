from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse


TARGET_GROUPS = [
    "info.gov.hk",
    "devb.gov.hk",
    "tel.directory.gov.hk",
    "archsd.gov.hk",
]


def _read_jsonl(path: Path) -> Iterable[tuple[int, dict[str, Any] | None, str | None]]:
    """Yield (line_no, record_or_none, error_or_none)."""
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as e:
                yield i, None, f"json_decode_error: {e}"
                continue
            if not isinstance(obj, dict):
                yield i, None, f"not_an_object: {type(obj).__name__}"
                continue
            yield i, obj, None


def _safe_parse_url(url: str) -> tuple[str | None, str | None]:
    s = (url or "").strip()
    if not s:
        return None, None
    try:
        p = urlparse(s)
    except Exception:
        return None, None
    host = (p.netloc or "").lower() or None
    path = p.path or ""
    return host, path


def _bucket_for_record(rec: dict[str, Any]) -> str:
    # tel_directory records often point to www.directory.gov.hk/details.jsp.
    # We still want to group these under tel.directory.gov.hk.
    if (rec.get("source") or "").strip().lower() == "tel_directory":
        return "tel.directory.gov.hk"

    url = str(rec.get("url") or "").strip()
    host, _ = _safe_parse_url(url)

    if host:
        for target in TARGET_GROUPS:
            if host == target or host.endswith("." + target):
                return target

    return "other"


_ISO_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def _parse_discovered_at(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if not _ISO_DT_RE.match(s):
        return None
    # Support Z and offsets
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _shorten(s: str, n: int = 160) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _pct(n: int, d: int) -> str:
    if d <= 0:
        return "0%"
    return f"{(100.0 * n / d):.1f}%"


@dataclass
class _Anomaly:
    kind: str
    line_no: int
    detail: str


def _meta_keys(meta: Any) -> set[str]:
    if not isinstance(meta, dict):
        return set()
    return {str(k) for k in meta.keys()}


def _meta_value_type(meta: Any, key: str) -> str:
    if not isinstance(meta, dict) or key not in meta:
        return "missing"
    v = meta[key]
    if v is None:
        return "null"
    return type(v).__name__


def build_report(path: Path) -> dict[str, Any]:
    totals = {
        "lines_total": 0,
        "records_total": 0,
        "records_by_group": Counter(),
        "records_by_source": Counter(),
        "parse_errors": 0,
    }

    top_level_keys_by_group: dict[str, Counter[frozenset[str]]] = defaultdict(Counter)
    meta_keys_by_group: dict[str, Counter[frozenset[str]]] = defaultdict(Counter)
    url_hosts_by_group: dict[str, Counter[str]] = defaultdict(Counter)
    url_exts_by_group: dict[str, Counter[str]] = defaultdict(Counter)
    meta_key_freq_by_group: dict[str, Counter[str]] = defaultdict(Counter)

    missing_name_by_group: Counter[str] = Counter()
    missing_meta_by_group: Counter[str] = Counter()
    url_host_mismatch_by_group: Counter[str] = Counter()

    unique_urls_by_group: dict[str, set[str]] = defaultdict(set)
    dup_urls_by_group: dict[str, Counter[str]] = defaultdict(Counter)

    discovered_times_by_group: dict[str, list[datetime]] = defaultdict(list)
    record_sizes: list[int] = []

    anomalies: list[_Anomaly] = []

    for line_no, rec, err in _read_jsonl(path):
        totals["lines_total"] += 1
        if err:
            totals["parse_errors"] += 1
            anomalies.append(_Anomaly("parse_error", line_no, err))
            continue

        assert rec is not None
        totals["records_total"] += 1

        group = _bucket_for_record(rec)
        totals["records_by_group"][group] += 1

        src = str(rec.get("source") or "").strip() or "(missing)"
        totals["records_by_source"][src] += 1

        keys = frozenset(str(k) for k in rec.keys())
        top_level_keys_by_group[group][keys] += 1

        meta = rec.get("meta")
        meta_sig = frozenset(_meta_keys(meta))
        meta_keys_by_group[group][meta_sig] += 1
        if isinstance(meta, dict):
            for k in meta.keys():
                meta_key_freq_by_group[group][str(k)] += 1

        url = str(rec.get("url") or "").strip()
        if not url:
            anomalies.append(_Anomaly("missing_url", line_no, "url is empty"))
        else:
            host, url_path = _safe_parse_url(url)
            if not host:
                anomalies.append(_Anomaly("bad_url", line_no, _shorten(url)))
            else:
                url_hosts_by_group[group][host] += 1

                # For non-tel buckets, ensure the URL host matches the group.
                if group in TARGET_GROUPS and group != "tel.directory.gov.hk":
                    if not (host == group or host.endswith("." + group)):
                        url_host_mismatch_by_group[group] += 1

            ext = ""
            if url_path:
                # Quick extension heuristic (includes .html, .pdf, .jsp, etc)
                last = url_path.rsplit("/", 1)[-1]
                if "." in last:
                    ext = "." + last.rsplit(".", 1)[-1].lower()
            url_exts_by_group[group][ext or "(no_ext)"] += 1

            if url in unique_urls_by_group[group]:
                dup_urls_by_group[group][url] += 1
            else:
                unique_urls_by_group[group].add(url)

        if "meta" not in rec:
            anomalies.append(_Anomaly("missing_meta", line_no, "meta key missing"))
            missing_meta_by_group[group] += 1
        elif meta is not None and not isinstance(meta, dict):
            anomalies.append(
                _Anomaly("meta_not_object", line_no, f"meta is {type(meta).__name__}")
            )
            missing_meta_by_group[group] += 1
        elif meta is None:
            missing_meta_by_group[group] += 1

        if "name" not in rec:
            anomalies.append(_Anomaly("missing_name_key", line_no, "name key missing"))
            missing_name_by_group[group] += 1
        else:
            nm = rec.get("name")
            if nm is None or (isinstance(nm, str) and not nm.strip()):
                missing_name_by_group[group] += 1

        dt = _parse_discovered_at(rec.get("discovered_at_utc"))
        if dt is None:
            anomalies.append(
                _Anomaly(
                    "bad_discovered_at_utc",
                    line_no,
                    _shorten(str(rec.get("discovered_at_utc"))),
                )
            )
        else:
            discovered_times_by_group[group].append(dt)

        # Group-specific sanity checks
        if group == "tel.directory.gov.hk":
            if src != "tel_directory":
                anomalies.append(
                    _Anomaly("tel_group_unexpected_source", line_no, f"source={src}")
                )
            if isinstance(meta, dict):
                if not (meta.get("dedup_key") or ""):
                    anomalies.append(
                        _Anomaly("tel_missing_dedup_key", line_no, _shorten(url))
                    )
                if not (meta.get("office_tel") or ""):
                    anomalies.append(
                        _Anomaly("tel_missing_office_tel", line_no, _shorten(url))
                    )

        if group in ("devb.gov.hk", "archsd.gov.hk"):
            # Often these are documents; flag missing name only if URL looks like a document.
            _, pth = _safe_parse_url(url)
            if pth and any(pth.lower().endswith(x) for x in (".pdf", ".doc", ".docx")):
                # allowed to have name=None
                pass

        try:
            record_sizes.append(len(json.dumps(rec, ensure_ascii=False)))
        except Exception:
            pass

    def _time_range(dts: list[datetime]) -> dict[str, Any] | None:
        if not dts:
            return None
        dts_sorted = sorted(dts)
        return {
            "min": dts_sorted[0].isoformat(),
            "max": dts_sorted[-1].isoformat(),
            "count": len(dts_sorted),
        }

    # Summaries
    per_group: dict[str, Any] = {}
    for group, n in totals["records_by_group"].items():
        # Top schema signatures (top-level keys + meta keys)
        top_schemas = [
            {"keys": sorted(list(sig)), "count": c}
            for sig, c in top_level_keys_by_group[group].most_common(5)
        ]
        top_meta_schemas = [
            {"meta_keys": sorted(list(sig)), "count": c}
            for sig, c in meta_keys_by_group[group].most_common(5)
        ]

        host_counts = url_hosts_by_group[group].most_common(10)
        ext_counts = url_exts_by_group[group].most_common(10)

        meta_key_counts = meta_key_freq_by_group[group].most_common(12)
        meta_schema_variants = len(meta_keys_by_group[group])

        unique_n = len(unique_urls_by_group[group])
        dup_n = sum(dup_urls_by_group[group].values())

        per_group[group] = {
            "records": n,
            "unique_urls": unique_n,
            "duplicate_url_hits": dup_n,
            "missing_name": int(missing_name_by_group[group]),
            "missing_meta": int(missing_meta_by_group[group]),
            "url_host_mismatch": int(url_host_mismatch_by_group[group]),
            "top_level_schema_signatures": top_schemas,
            "meta_schema_signatures": top_meta_schemas,
            "meta_schema_variants": meta_schema_variants,
            "top_meta_keys": meta_key_counts,
            "top_hosts": host_counts,
            "top_extensions": ext_counts,
            "discovered_at_range": _time_range(discovered_times_by_group[group]),
        }

    # General stats
    size_stats: dict[str, Any] = {}
    if record_sizes:
        size_stats = {
            "min": min(record_sizes),
            "max": max(record_sizes),
            "mean": statistics.mean(record_sizes),
            "p50": statistics.median(record_sizes),
            "p95": (
                statistics.quantiles(record_sizes, n=20)[-1]
                if len(record_sizes) >= 20
                else None
            ),
        }

    anomalies_by_kind = Counter(a.kind for a in anomalies)
    # keep up to 30 concrete examples
    examples: list[dict[str, Any]] = []
    for a in anomalies[:30]:
        examples.append({"kind": a.kind, "line": a.line_no, "detail": a.detail})

    return {
        "input": str(path),
        "totals": {
            **totals,
            "records_by_group": dict(totals["records_by_group"]),
            "records_by_source": dict(totals["records_by_source"]),
        },
        "groups": per_group,
        "general": {
            "record_size_bytes": size_stats,
            "anomaly_counts": dict(anomalies_by_kind),
            "anomaly_examples": examples,
        },
    }


def _print_report(report: dict[str, Any]) -> None:
    totals = report["totals"]

    print(f"Input: {report['input']}")
    print(
        f"Total records: {totals['records_total']} (parse_errors={totals['parse_errors']})"
    )

    print("\nRecords by website group:")
    by_group = totals["records_by_group"]
    total = totals["records_total"] or 1
    for k in sorted(by_group.keys()):
        v = by_group[k]
        print(f"- {k}: {v} ({_pct(v, total)})")

    print("\nCrawler sources (top 15):")
    for src, c in Counter(totals["records_by_source"]).most_common(15):
        print(f"- {src}: {c}")

    print("\nPer-group format + schema summary:")
    for group, g in report["groups"].items():
        print(f"\n[{group}]")
        print(
            f"- records: {g['records']} | unique_urls: {g['unique_urls']} | dup_url_hits: {g['duplicate_url_hits']}"
        )

        print(
            "- completeness: "
            f"missing_name={g.get('missing_name', 0)} ({_pct(g.get('missing_name', 0), g['records'])}) "
            f"missing_meta={g.get('missing_meta', 0)} ({_pct(g.get('missing_meta', 0), g['records'])})"
        )
        if group in TARGET_GROUPS and group != "tel.directory.gov.hk":
            m = g.get("url_host_mismatch", 0)
            if m:
                print(f"- url host mismatches vs group: {m} ({_pct(m, g['records'])})")

        dr = g.get("discovered_at_range")
        if dr:
            print(f"- discovered_at_utc: {dr['min']} .. {dr['max']}")

        print("- top url hosts:")
        for host, c in g.get("top_hosts", [])[:6]:
            print(f"  - {host}: {c}")

        print("- top url extensions:")
        for ext, c in g.get("top_extensions", [])[:6]:
            print(f"  - {ext}: {c}")

        print("- top-level key signatures (urls.jsonl record format):")
        for sig in g.get("top_level_schema_signatures", [])[:3]:
            print(f"  - count={sig['count']} keys={sig['keys']}")

        print("- meta key signatures:")
        for sig in g.get("meta_schema_signatures", [])[:3]:
            print(f"  - count={sig['count']} meta_keys={sig['meta_keys']}")

        mv = g.get("meta_schema_variants")
        if mv is not None:
            print(f"- meta schema variants: {mv}")

        top_meta_keys = g.get("top_meta_keys") or []
        if top_meta_keys:
            print("- most common meta keys:")
            for k, c in top_meta_keys[:8]:
                print(f"  - {k}: {c}")

    gen = report["general"]
    print("\nGeneral stats:")
    rs = gen.get("record_size_bytes") or {}
    if rs:
        p95 = rs.get("p95")
        p95s = f"{p95:.1f}" if isinstance(p95, (float, int)) else "n/a"
        print(
            "- record JSON size (bytes): "
            f"min={rs.get('min')} p50={rs.get('p50')} mean={rs.get('mean'):.1f} p95={p95s} max={rs.get('max')}"
        )

    print("\nUnusual records / anomalies:")
    ac = gen.get("anomaly_counts") or {}
    if not ac:
        print("- none detected by current heuristics")
    else:
        for kind, c in Counter(ac).most_common(20):
            print(f"- {kind}: {c}")

    examples = gen.get("anomaly_examples") or []
    if examples:
        print("\nExamples (first 30):")
        for ex in examples:
            print(
                f"- line {ex['line']}: {ex['kind']} -> {_shorten(str(ex['detail']), 200)}"
            )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Generate stats for urls.jsonl crawl outputs"
    )
    ap.add_argument(
        "--in",
        dest="input_path",
        default="local-data/latest/urls.jsonl",
        help="Path to urls.jsonl (default: local-data/latest/urls.jsonl)",
    )
    ap.add_argument(
        "--out-json",
        default="",
        help="Optional path to write machine-readable JSON report",
    )

    args = ap.parse_args()
    path = Path(args.input_path)
    if not path.exists():
        raise SystemExit(f"Input file not found: {path}")

    report = build_report(path)
    _print_report(report)

    out_json = (args.out_json or "").strip()
    if out_json:
        out_path = Path(out_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nWrote JSON report to: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
