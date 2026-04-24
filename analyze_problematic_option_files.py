import argparse
import csv
import glob
import os
from collections import Counter, defaultdict
from datetime import datetime

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

DEFAULT_DATA_ROOT = "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322"
DEFAULT_RAW_PATH = os.path.join(DEFAULT_DATA_ROOT, "raw_percent")
DEFAULT_AUDIT_DIR = os.path.join(DEFAULT_DATA_ROOT, "audit")
EXPECTED_TENORS = ['1w', '2w', '3w', '1m', '2m', '3m', '6m', '1y', '2y']
MASTER_KEY_FIELDS = ["ticker", "date", "sf", "cp", "metric"]


def progress_iter(iterable, total, desc):
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)


def parse_args():
    parser = argparse.ArgumentParser(description="Consolidate and revalidate problematic option files.")
    parser.add_argument("--audit-dir", default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--raw-path", default=DEFAULT_RAW_PATH)
    parser.add_argument(
        "--output-prefix",
        default=datetime.now().strftime("problematic_option_analysis_%Y%m%d_%H%M%S"),
        help="Prefix for generated CSV files inside audit dir.",
    )
    return parser.parse_args()


def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def latest_problem_files(audit_dir):
    missing_paths = sorted(glob.glob(os.path.join(audit_dir, "missing_files_*.csv")))
    bad_paths = sorted(glob.glob(os.path.join(audit_dir, "bad_files_*.csv")))
    return missing_paths, bad_paths


def pending_week_map(audit_dir):
    pending_path = os.path.join(audit_dir, "pending_weekly_backfill.csv")
    mapping = {}
    if not os.path.exists(pending_path):
        return mapping
    for row in read_csv_rows(pending_path):
        summary_path = row.get("summary_path", "")
        missing_path = row.get("missing_path", "")
        bad_path = row.get("bad_path", "")
        for key in [summary_path, missing_path, bad_path]:
            if key:
                mapping[key] = row
    return mapping


def consolidate_problem_rows(paths, source_kind, pending_map):
    consolidated = []
    for path in paths:
        pending = pending_map.get(path, {})
        try:
            rows = read_csv_rows(path)
        except FileNotFoundError:
            continue
        for row in rows:
            consolidated.append({
                **row,
                "source_kind": source_kind,
                "source_audit_path": path,
                "pending_chunk_start": pending.get("chunk_start", ""),
                "pending_chunk_end": pending.get("chunk_end", ""),
                "pending_problematic_pct": pending.get("problematic_pct", ""),
            })
    return consolidated


def normalize_problem_rows(rows):
    grouped = {}
    for row in rows:
        key = tuple(row.get(field, "") for field in MASTER_KEY_FIELDS)
        entry = grouped.setdefault(
            key,
            {
                "ticker": row.get("ticker", ""),
                "date": row.get("date", ""),
                "sf": row.get("sf", ""),
                "cp": row.get("cp", ""),
                "metric": row.get("metric", ""),
                "filename": row.get("filename", ""),
                "path": row.get("path", ""),
                "first_seen_source_kind": row.get("source_kind", ""),
                "source_kinds": set(),
                "statuses_seen": set(),
                "seen_count": 0,
                "audit_paths": set(),
                "pending_windows": set(),
            },
        )
        entry["source_kinds"].add(row.get("source_kind", ""))
        entry["statuses_seen"].add(row.get("status", row.get("source_kind", "")))
        entry["audit_paths"].add(row.get("source_audit_path", ""))
        if row.get("pending_chunk_start") and row.get("pending_chunk_end"):
            entry["pending_windows"].add(f"{row['pending_chunk_start']}->{row['pending_chunk_end']}")
        entry["seen_count"] += 1
    normalized = []
    for entry in grouped.values():
        normalized.append({
            **{k: v for k, v in entry.items() if k not in {"source_kinds", "statuses_seen", "audit_paths", "pending_windows"}},
            "source_kinds": ",".join(sorted(filter(None, entry["source_kinds"]))),
            "statuses_seen": ",".join(sorted(filter(None, entry["statuses_seen"]))),
            "audit_paths": "|".join(sorted(filter(None, entry["audit_paths"]))),
            "pending_windows": "|".join(sorted(filter(None, entry["pending_windows"]))),
        })
    return normalized


def validate_file(path):
    result = {
        "exists_now": os.path.exists(path),
        "readable_now": False,
        "file_size_bytes_now": 0,
        "row_count_now": 0,
        "column_count_now": 0,
        "has_tenor_column_now": False,
        "tenor_count_now": 0,
        "missing_expected_tenors_now": "",
        "non_null_numeric_count_now": 0,
        "current_status": "confirmed_missing",
        "current_error": "",
        "revalidation_class": "confirmed_missing",
    }
    if not result["exists_now"]:
        return result
    result["file_size_bytes_now"] = os.path.getsize(path)
    if result["file_size_bytes_now"] == 0:
        result["current_status"] = "empty_file"
        result["revalidation_class"] = "confirmed_bad"
        return result
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        result["current_status"] = "unreadable_csv"
        result["current_error"] = repr(exc)
        result["revalidation_class"] = "confirmed_bad"
        return result
    result["readable_now"] = True
    result["row_count_now"] = len(df)
    result["column_count_now"] = len(df.columns)
    result["has_tenor_column_now"] = "Tenor" in df.columns
    if not result["has_tenor_column_now"]:
        result["current_status"] = "missing_tenor_column"
        result["revalidation_class"] = "confirmed_bad"
        return result
    tenors_present = [str(value) for value in df["Tenor"].dropna().tolist()]
    result["tenor_count_now"] = len(tenors_present)
    missing_tenors = [tenor for tenor in EXPECTED_TENORS if tenor not in tenors_present]
    result["missing_expected_tenors_now"] = ",".join(missing_tenors)
    numeric_df = df.drop(columns=["Tenor"], errors="ignore").apply(pd.to_numeric, errors="coerce")
    result["non_null_numeric_count_now"] = int(numeric_df.notna().sum().sum())
    if result["row_count_now"] == 0:
        result["current_status"] = "no_rows"
        result["revalidation_class"] = "confirmed_bad"
    elif result["non_null_numeric_count_now"] == 0:
        result["current_status"] = "all_numeric_null"
        result["revalidation_class"] = "confirmed_bad"
    elif missing_tenors:
        result["current_status"] = "missing_tenors"
        result["revalidation_class"] = "needs_manual_review"
    else:
        result["current_status"] = "ok"
        result["revalidation_class"] = "recovered_ok"
    return result


def summarize(rows, group_fields):
    counters = defaultdict(Counter)
    for row in rows:
        key = tuple(row.get(field, "") for field in group_fields)
        counters[key][row.get("revalidation_class", "unknown")] += 1
        counters[key]["total"] += 1
    summary_rows = []
    for key, counter in sorted(counters.items()):
        row = {field: value for field, value in zip(group_fields, key)}
        row.update({
            "total": counter.get("total", 0),
            "confirmed_missing": counter.get("confirmed_missing", 0),
            "confirmed_bad": counter.get("confirmed_bad", 0),
            "needs_manual_review": counter.get("needs_manual_review", 0),
            "recovered_ok": counter.get("recovered_ok", 0),
        })
        summary_rows.append(row)
    return summary_rows


def main():
    args = parse_args()
    missing_paths, bad_paths = latest_problem_files(args.audit_dir)
    pending_map = pending_week_map(args.audit_dir)
    problem_rows = consolidate_problem_rows(missing_paths, "missing", pending_map)
    problem_rows += consolidate_problem_rows(bad_paths, "bad", pending_map)
    if not problem_rows:
        raise SystemExit("No missing/bad audit files found to analyze.")

    master_rows = normalize_problem_rows(problem_rows)
    master_rows.sort(key=lambda row: (row["ticker"], row["date"], row["sf"], row["cp"], row["metric"]))

    revalidated_rows = []
    for row in progress_iter(master_rows, total=len(master_rows), desc="Revalidate problematic files"):
        revalidated_rows.append({**row, **validate_file(row["path"])})

    master_path = os.path.join(args.audit_dir, f"{args.output_prefix}_master.csv")
    revalidated_path = os.path.join(args.audit_dir, f"{args.output_prefix}_revalidated.csv")
    summary_ticker_path = os.path.join(args.audit_dir, f"{args.output_prefix}_summary_by_ticker.csv")
    summary_type_path = os.path.join(args.audit_dir, f"{args.output_prefix}_summary_by_type.csv")

    write_csv(master_path, master_rows, list(master_rows[0].keys()))
    write_csv(revalidated_path, revalidated_rows, list(revalidated_rows[0].keys()))

    summary_by_ticker = summarize(revalidated_rows, ["ticker"])
    summary_by_type = summarize(revalidated_rows, ["sf", "cp", "metric"])
    write_csv(summary_ticker_path, summary_by_ticker, list(summary_by_ticker[0].keys()) if summary_by_ticker else ["ticker", "total", "confirmed_missing", "confirmed_bad", "needs_manual_review", "recovered_ok"])
    write_csv(summary_type_path, summary_by_type, list(summary_by_type[0].keys()) if summary_by_type else ["sf", "cp", "metric", "total", "confirmed_missing", "confirmed_bad", "needs_manual_review", "recovered_ok"])

    class_counter = Counter(row["revalidation_class"] for row in revalidated_rows)
    print(f"Master problematic file list: {master_path}")
    print(f"Revalidated file list: {revalidated_path}")
    print(f"Summary by ticker: {summary_ticker_path}")
    print(f"Summary by type: {summary_type_path}")
    print("Revalidation summary:")
    for key in ["confirmed_missing", "confirmed_bad", "needs_manual_review", "recovered_ok"]:
        print(f"  {key}: {class_counter.get(key, 0)}")


if __name__ == "__main__":
    main()
