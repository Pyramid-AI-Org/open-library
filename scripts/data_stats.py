from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse


# Generic outlier tuning for per-source scalar meta value checks.
MIN_ROWS_FOR_VALUE_OUTLIERS = 20
RARE_VALUE_RATIO = 0.02
MAX_DISTINCT_RATIO_FOR_OUTLIERS = 0.30
MIN_DOMINANT_SHARE_FOR_OUTLIERS = 0.30
MAX_ISSUE_EXAMPLES = 50


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


def _is_url_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    s = value.strip()
    if not s:
        return False
    try:
        p = urlparse(s)
    except Exception:
        return False
    return bool(p.scheme and p.netloc)


def _source_key(rec: dict[str, Any]) -> str:
    src = rec.get("source")
    if isinstance(src, str) and src.strip():
        return src.strip()
    return "(missing)"


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


_ISO_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def _parse_discovered_at(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if _ISO_DATE_RE.match(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    if not _ISO_DT_RE.match(s):
        return None
    # Support Z and offsets
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_publish_date_or_null(value: Any) -> bool:
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    s = value.strip()
    if not _ISO_DATE_RE.match(s):
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return False
    return True


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
class _Issue:
    severity: str
    kind: str
    line_no: int
    source: str
    field: str
    detail: str


def _meta_keys(meta: Any) -> set[str]:
    if not isinstance(meta, dict):
        return set()
    return {str(k) for k in meta.keys()}


def _value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, bool, int, float))


def _scalar_key(value: Any) -> str:
    if value is None:
        return "null:null"
    t = _value_type(value)
    if isinstance(value, str):
        return f"{t}:{value.strip()}"
    return f"{t}:{value}"


def _scalar_key_to_value_text(key: str) -> str:
    if ":" in key:
        return key.split(":", 1)[1]
    return key


def _display_value(value: Any, max_len: int = 120) -> str:
    try:
        if isinstance(value, str):
            return _shorten(value, max_len)
        return _shorten(json.dumps(value, ensure_ascii=False), max_len)
    except Exception:
        return _shorten(str(value), max_len)


def _add_issue(
    issues: list[_Issue],
    *,
    severity: str,
    kind: str,
    line_no: int,
    source: str,
    field: str,
    detail: str,
) -> None:
    issues.append(
        _Issue(
            severity=severity,
            kind=kind,
            line_no=line_no,
            source=source,
            field=field,
            detail=detail,
        )
    )


def _required_field_stats() -> dict[str, dict[str, int]]:
    # missing: key absent, null: explicit null/empty string for string fields,
    # invalid: present but wrong type/format.
    return {
        "url": {"missing": 0, "null": 0, "invalid": 0},
        "name": {"missing": 0, "null": 0, "invalid": 0},
        "discovered_at_utc": {"missing": 0, "null": 0, "invalid": 0},
        "publish_date": {"missing": 0, "null": 0, "invalid": 0},
        "source": {"missing": 0, "null": 0, "invalid": 0},
        "meta.discovered_from": {"missing": 0, "null": 0, "invalid": 0},
    }


def build_report(path: Path) -> dict[str, Any]:
    totals = {
        "lines_total": 0,
        "records_total": 0,
        "records_by_source": Counter(),
        "parse_errors": 0,
    }

    top_level_keys_by_source: dict[str, Counter[frozenset[str]]] = defaultdict(Counter)
    meta_keys_by_source: dict[str, Counter[frozenset[str]]] = defaultdict(Counter)
    url_hosts_by_source: dict[str, Counter[str]] = defaultdict(Counter)
    url_exts_by_source: dict[str, Counter[str]] = defaultdict(Counter)
    meta_key_freq_by_source: dict[str, Counter[str]] = defaultdict(Counter)
    meta_type_freq_by_source: dict[str, dict[str, Counter[str]]] = defaultdict(
        lambda: defaultdict(Counter)
    )
    meta_scalar_values_by_source: dict[str, dict[str, Counter[str]]] = defaultdict(
        lambda: defaultdict(Counter)
    )

    required_by_source: dict[str, dict[str, dict[str, int]]] = defaultdict(
        _required_field_stats
    )
    unique_urls_by_source: dict[str, set[str]] = defaultdict(set)
    dup_urls_by_source: dict[str, Counter[str]] = defaultdict(Counter)

    discovered_times_by_source: dict[str, list[datetime]] = defaultdict(list)
    issues: list[_Issue] = []
    record_sizes: list[int] = []

    records: list[tuple[int, str, dict[str, Any]]] = []

    for line_no, rec, err in _read_jsonl(path):
        totals["lines_total"] += 1
        if err:
            totals["parse_errors"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="parse_error",
                line_no=line_no,
                source="(parse_error)",
                field="record",
                detail=err,
            )
            continue

        assert rec is not None
        totals["records_total"] += 1

        src = _source_key(rec)
        records.append((line_no, src, rec))
        totals["records_by_source"][src] += 1

        keys = frozenset(str(k) for k in rec.keys())
        top_level_keys_by_source[src][keys] += 1

        meta = rec.get("meta")
        meta_sig = frozenset(_meta_keys(meta))
        meta_keys_by_source[src][meta_sig] += 1
        if isinstance(meta, dict):
            for k, v in meta.items():
                kk = str(k)
                meta_key_freq_by_source[src][kk] += 1
                t = _value_type(v)
                meta_type_freq_by_source[src][kk][t] += 1
                if _is_scalar(v):
                    meta_scalar_values_by_source[src][kk][_scalar_key(v)] += 1

        # Gather quick source-level distributions and duplicates.
        raw_url = rec.get("url")
        url = raw_url.strip() if isinstance(raw_url, str) else ""
        if url:
            host, url_path = _safe_parse_url(url)
            if host:
                url_hosts_by_source[src][host] += 1

            ext = ""
            if url_path:
                last = url_path.rsplit("/", 1)[-1]
                if "." in last:
                    ext = "." + last.rsplit(".", 1)[-1].lower()
            url_exts_by_source[src][ext or "(no_ext)"] += 1

            if url in unique_urls_by_source[src]:
                dup_urls_by_source[src][url] += 1
            else:
                unique_urls_by_source[src].add(url)

        try:
            record_sizes.append(len(json.dumps(rec, ensure_ascii=False)))
        except Exception:
            pass

    dominant_meta_type_by_source: dict[str, dict[str, tuple[str, int]]] = defaultdict(
        dict
    )
    rare_meta_values_by_source: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for src, per_key in meta_type_freq_by_source.items():
        for key, counts in per_key.items():
            if counts:
                dominant_meta_type_by_source[src][key] = counts.most_common(1)[0]

    for src, per_key_values in meta_scalar_values_by_source.items():
        for key, counts in per_key_values.items():
            total = sum(counts.values())
            if total < MIN_ROWS_FOR_VALUE_OUTLIERS or len(counts) <= 1:
                continue
            distinct_ratio = len(counts) / float(total)
            if distinct_ratio > MAX_DISTINCT_RATIO_FOR_OUTLIERS:
                continue
            rare_cutoff = max(1, int(total * RARE_VALUE_RATIO))
            common_value, common_count = counts.most_common(1)[0]
            dominant_share = common_count / float(total)
            if dominant_share < MIN_DOMINANT_SHARE_FOR_OUTLIERS:
                continue
            for value_key, c in counts.items():
                if value_key == common_value:
                    continue
                if c <= rare_cutoff and c < common_count:
                    rare_meta_values_by_source[src][key].add(value_key)

    for line_no, src, rec in records:
        req = required_by_source[src]

        # url: required non-empty URL string
        if "url" not in rec:
            req["url"]["missing"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="missing_required_field",
                line_no=line_no,
                source=src,
                field="url",
                detail="missing required field",
            )
        else:
            v = rec.get("url")
            if v is None:
                req["url"]["null"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="null_required_field",
                    line_no=line_no,
                    source=src,
                    field="url",
                    detail="url is null",
                )
            elif not isinstance(v, str) or not v.strip():
                req["url"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_required_type_or_empty",
                    line_no=line_no,
                    source=src,
                    field="url",
                    detail=f"expected non-empty string URL, got {_value_type(v)}",
                )
            elif not _is_url_string(v):
                req["url"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_url_format",
                    line_no=line_no,
                    source=src,
                    field="url",
                    detail=_display_value(v),
                )

        # name: required non-empty string
        if "name" not in rec:
            req["name"]["missing"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="missing_required_field",
                line_no=line_no,
                source=src,
                field="name",
                detail="missing required field",
            )
        else:
            v = rec.get("name")
            if v is None:
                req["name"]["null"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="null_required_field",
                    line_no=line_no,
                    source=src,
                    field="name",
                    detail="name is null",
                )
            elif not isinstance(v, str) or not v.strip():
                req["name"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_required_type_or_empty",
                    line_no=line_no,
                    source=src,
                    field="name",
                    detail=f"expected non-empty string, got {_value_type(v)}",
                )

        # discovered_at_utc: required datetime string
        if "discovered_at_utc" not in rec:
            req["discovered_at_utc"]["missing"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="missing_required_field",
                line_no=line_no,
                source=src,
                field="discovered_at_utc",
                detail="missing required field",
            )
        else:
            v = rec.get("discovered_at_utc")
            if v is None:
                req["discovered_at_utc"]["null"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="null_required_field",
                    line_no=line_no,
                    source=src,
                    field="discovered_at_utc",
                    detail="discovered_at_utc is null",
                )
            elif not isinstance(v, str) or not v.strip():
                req["discovered_at_utc"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_required_type_or_empty",
                    line_no=line_no,
                    source=src,
                    field="discovered_at_utc",
                    detail=f"expected non-empty datetime string, got {_value_type(v)}",
                )
            else:
                dt = _parse_discovered_at(v)
                if dt is None:
                    req["discovered_at_utc"]["invalid"] += 1
                    _add_issue(
                        issues,
                        severity="error",
                        kind="invalid_datetime_format",
                        line_no=line_no,
                        source=src,
                        field="discovered_at_utc",
                        detail=_display_value(v),
                    )
                else:
                    discovered_times_by_source[src].append(dt)

        # publish_date: required key, value must be yyyy-mm-dd or null
        if "publish_date" not in rec:
            req["publish_date"]["missing"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="missing_required_field",
                line_no=line_no,
                source=src,
                field="publish_date",
                detail="missing required field",
            )
        else:
            v = rec.get("publish_date")
            if v is None:
                req["publish_date"]["null"] += 1
            elif not _is_publish_date_or_null(v):
                req["publish_date"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_publish_date_format",
                    line_no=line_no,
                    source=src,
                    field="publish_date",
                    detail=_display_value(v),
                )

        # source: required non-empty string
        if "source" not in rec:
            req["source"]["missing"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="missing_required_field",
                line_no=line_no,
                source=src,
                field="source",
                detail="missing required field",
            )
        else:
            v = rec.get("source")
            if v is None:
                req["source"]["null"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="null_required_field",
                    line_no=line_no,
                    source=src,
                    field="source",
                    detail="source is null",
                )
            elif not isinstance(v, str) or not v.strip():
                req["source"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_required_type_or_empty",
                    line_no=line_no,
                    source=src,
                    field="source",
                    detail=f"expected non-empty string, got {_value_type(v)}",
                )

        # meta.discovered_from: required string URL
        if "meta" not in rec:
            req["meta.discovered_from"]["missing"] += 1
            _add_issue(
                issues,
                severity="error",
                kind="missing_required_field",
                line_no=line_no,
                source=src,
                field="meta",
                detail="missing required object for meta.discovered_from",
            )
            meta = None
        else:
            meta = rec.get("meta")
            if meta is None:
                req["meta.discovered_from"]["null"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="null_required_field",
                    line_no=line_no,
                    source=src,
                    field="meta",
                    detail="meta is null",
                )
            elif not isinstance(meta, dict):
                req["meta.discovered_from"]["invalid"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="invalid_required_type_or_empty",
                    line_no=line_no,
                    source=src,
                    field="meta",
                    detail=f"expected object, got {_value_type(meta)}",
                )

        if isinstance(meta, dict):
            if "discovered_from" not in meta:
                req["meta.discovered_from"]["missing"] += 1
                _add_issue(
                    issues,
                    severity="error",
                    kind="missing_required_field",
                    line_no=line_no,
                    source=src,
                    field="meta.discovered_from",
                    detail="missing required field",
                )
            else:
                mv = meta.get("discovered_from")
                if mv is None:
                    req["meta.discovered_from"]["null"] += 1
                    _add_issue(
                        issues,
                        severity="error",
                        kind="null_required_field",
                        line_no=line_no,
                        source=src,
                        field="meta.discovered_from",
                        detail="meta.discovered_from is null",
                    )
                elif not isinstance(mv, str) or not mv.strip():
                    req["meta.discovered_from"]["invalid"] += 1
                    _add_issue(
                        issues,
                        severity="error",
                        kind="invalid_required_type_or_empty",
                        line_no=line_no,
                        source=src,
                        field="meta.discovered_from",
                        detail=f"expected non-empty string URL, got {_value_type(mv)}",
                    )
                elif not _is_url_string(mv):
                    req["meta.discovered_from"]["invalid"] += 1
                    _add_issue(
                        issues,
                        severity="error",
                        kind="invalid_url_format",
                        line_no=line_no,
                        source=src,
                        field="meta.discovered_from",
                        detail=_display_value(mv),
                    )

            # Source-local unusual type/value checks for meta fields.
            for mk, mv in meta.items():
                key = str(mk)
                dominant = dominant_meta_type_by_source.get(src, {}).get(key)
                if dominant:
                    dom_type, dom_count = dominant
                    actual_type = _value_type(mv)
                    if (
                        actual_type != dom_type
                        and dom_count >= 5
                        and meta_type_freq_by_source[src][key][actual_type] <= 2
                    ):
                        _add_issue(
                            issues,
                            severity="warning",
                            kind="meta_type_outlier",
                            line_no=line_no,
                            source=src,
                            field=f"meta.{key}",
                            detail=(
                                f"value type {actual_type} differs from dominant "
                                f"type {dom_type} in source"
                            ),
                        )

                if _is_scalar(mv):
                    v_key = _scalar_key(mv)
                    if v_key in rare_meta_values_by_source.get(src, {}).get(key, set()):
                        _add_issue(
                            issues,
                            severity="warning",
                            kind="meta_value_outlier",
                            line_no=line_no,
                            source=src,
                            field=f"meta.{key}",
                            detail=f"rare value in source: {_display_value(mv)}",
                        )

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
    per_source: dict[str, Any] = {}
    for src, n in totals["records_by_source"].items():
        top_schemas = [
            {"keys": sorted(list(sig)), "count": c}
            for sig, c in top_level_keys_by_source[src].most_common(5)
        ]
        top_meta_schemas = [
            {"meta_keys": sorted(list(sig)), "count": c}
            for sig, c in meta_keys_by_source[src].most_common(5)
        ]

        host_counts = url_hosts_by_source[src].most_common(10)
        ext_counts = url_exts_by_source[src].most_common(10)

        meta_key_counts = meta_key_freq_by_source[src].most_common(12)
        meta_schema_variants = len(meta_keys_by_source[src])

        meta_type_variation = {}
        for key, counts in meta_type_freq_by_source[src].items():
            if len(counts) > 1:
                meta_type_variation[key] = dict(counts)

        top_meta_values = {}
        for key, counts in meta_scalar_values_by_source[src].items():
            if counts:
                top_meta_values[key] = [
                    {"value": _shorten(_scalar_key_to_value_text(v), 80), "count": c}
                    for v, c in counts.most_common(5)
                ]

        unique_n = len(unique_urls_by_source[src])
        dup_n = sum(dup_urls_by_source[src].values())

        issue_counts = Counter(
            i.kind
            for i in issues
            if i.source == src and i.severity in {"error", "warning"}
        )
        errors = sum(1 for i in issues if i.source == src and i.severity == "error")
        warnings = sum(1 for i in issues if i.source == src and i.severity == "warning")

        per_source[src] = {
            "records": n,
            "unique_urls": unique_n,
            "duplicate_url_hits": dup_n,
            "issue_counts": {
                "errors": errors,
                "warnings": warnings,
                "by_kind": dict(issue_counts),
            },
            "required_field_null_missing_invalid": required_by_source[src],
            "top_level_schema_signatures": top_schemas,
            "meta_schema_signatures": top_meta_schemas,
            "meta_schema_variants": meta_schema_variants,
            "meta_type_variation": meta_type_variation,
            "meta_top_scalar_values": top_meta_values,
            "top_meta_keys": meta_key_counts,
            "top_hosts": host_counts,
            "top_extensions": ext_counts,
            "discovered_at_range": _time_range(discovered_times_by_source[src]),
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

    issues_by_kind = Counter(f"{i.severity}:{i.kind}" for i in issues)
    examples: list[dict[str, Any]] = []
    for i in issues[:MAX_ISSUE_EXAMPLES]:
        examples.append(
            {
                "severity": i.severity,
                "kind": i.kind,
                "source": i.source,
                "field": i.field,
                "line": i.line_no,
                "detail": i.detail,
            }
        )

    severity_counts = Counter(i.severity for i in issues)

    return {
        "input": str(path),
        "totals": {
            **totals,
            "records_by_source": dict(totals["records_by_source"]),
        },
        "sources": per_source,
        "general": {
            "record_size_bytes": size_stats,
            "issue_counts": {
                "by_severity": dict(severity_counts),
                "by_severity_and_kind": dict(issues_by_kind),
            },
            "issue_examples": examples,
        },
    }


def _print_report(report: dict[str, Any]) -> None:
    totals = report["totals"]

    print(f"Input: {report['input']}")
    print(
        f"Total records: {totals['records_total']} (parse_errors={totals['parse_errors']})"
    )

    print("\nRecords by source:")
    by_source = totals["records_by_source"]
    total = totals["records_total"] or 1
    for k in sorted(by_source.keys()):
        v = by_source[k]
        print(f"- {k}: {v} ({_pct(v, total)})")

    print("\nPer-source schema and validation summary:")
    for src, g in report["sources"].items():
        print(f"\n[{src}]")
        print(
            f"- records: {g['records']} | unique_urls: {g['unique_urls']} | dup_url_hits: {g['duplicate_url_hits']}"
        )

        ic = g.get("issue_counts") or {}
        print(
            f"- issues: errors={ic.get('errors', 0)} warnings={ic.get('warnings', 0)}"
        )

        rq = g.get("required_field_null_missing_invalid") or {}
        print("- required field quality (missing/null/invalid):")
        for f in (
            "url",
            "name",
            "discovered_at_utc",
            "publish_date",
            "source",
            "meta.discovered_from",
        ):
            st = rq.get(f) or {}
            print(
                "  - "
                f"{f}: missing={st.get('missing', 0)} null={st.get('null', 0)} invalid={st.get('invalid', 0)}"
            )

        print(
            "- schema variation: "
            f"top-level variants={len(g.get('top_level_schema_signatures', []))} "
            f"meta variants={g.get('meta_schema_variants', 0)}"
        )

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

        mtv = g.get("meta_type_variation") or {}
        if mtv:
            print("- meta keys with type variation:")
            for key, counts in list(mtv.items())[:8]:
                print(f"  - {key}: {counts}")

        top_meta_keys = g.get("top_meta_keys") or []
        if top_meta_keys:
            print("- most common meta keys:")
            for k, c in top_meta_keys[:8]:
                print(f"  - {k}: {c}")

        top_meta_vals = g.get("meta_top_scalar_values") or {}
        if top_meta_vals:
            print("- top scalar meta values (sample):")
            for k, vals in list(top_meta_vals.items())[:6]:
                pretty = ", ".join(f"{v['value']} ({v['count']})" for v in vals[:3])
                print(f"  - {k}: {pretty}")

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

    print("\nUnusual records / issues:")
    ic = gen.get("issue_counts") or {}
    ac = ic.get("by_severity_and_kind") or {}
    if not ac:
        print("- none detected by current heuristics")
    else:
        for kind, c in Counter(ac).most_common(20):
            print(f"- {kind}: {c}")

    examples = gen.get("issue_examples") or []
    if examples:
        print(f"\nExamples (first {len(examples)}):")
        for ex in examples:
            print(
                "- "
                f"line {ex['line']} [{ex['severity']}] {ex['source']} {ex['field']} "
                f"{ex['kind']} -> {_shorten(str(ex['detail']), 200)}"
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
