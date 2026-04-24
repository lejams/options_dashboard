import argparse
import csv
import os
from collections import Counter
from datetime import datetime

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def progress_iter(iterable, total, desc):
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)

DEFAULT_DATA_ROOT = "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322"
DEFAULT_RAW_PATH = os.path.join(DEFAULT_DATA_ROOT, "raw_percent")
DEFAULT_AUDIT_DIR = os.path.join(DEFAULT_DATA_ROOT, "audit")
DEFAULT_TICKERS = [
    'SPX', 'QQQ US', 'DIA US', 'IWM US', 'GLD US', 'XLF US', 'XLE US', 'XLC US',
    'XLP US', 'XLV US', 'IYR US', 'HYG US', 'EEM US', 'FXI US', 'EWZ US', 'EWI US',
    'TLT UQ', 'XLK UP', 'XHB UP', 'SLV UP', 'USO UP', 'SX5E', 'NKY', 'NDX', 'RTY',
    'DAX', 'UKX', 'SMI', 'HSCEI', 'HSI', 'KOSPI2', 'AS51'
]
EXPECTED_TENORS = ['1w', '2w', '3w', '1m', '2m', '3m', '6m', '1y', '2y']
SF_OPTIONS = ['spot', 'fwd']
CP_OPTIONS = ['Call', 'Put']
METRIC_OPTIONS = ['percent', 'vol']


def parse_args():
    parser = argparse.ArgumentParser(description="Verify fetched options raw data coverage and file quality.")
    parser.add_argument("--start-date", required=True, help="Start business date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="End business date in YYYY-MM-DD format.")
    parser.add_argument(
        "--tickers",
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated ticker list to verify. Defaults to the full active options universe.",
    )
    parser.add_argument("--raw-path", default=DEFAULT_RAW_PATH, help="Folder containing raw option CSV files.")
    parser.add_argument("--audit-dir", default=DEFAULT_AUDIT_DIR, help="Folder where audit CSVs will be written.")
    return parser.parse_args()


def business_dates(start_date: str, end_date: str):
    dates = pd.bdate_range(start=start_date, end=end_date)
    return [d.strftime("%Y-%m-%d") for d in dates]


def expected_filename(ticker: str, sf: str, cp: str, metric: str, date_str: str):
    return f"{ticker}_{sf}_{cp}_option_{metric}_{date_str}.csv"


def validate_file(path: str):
    result = {
        "exists": os.path.exists(path),
        "readable": False,
        "file_size_bytes": 0,
        "row_count": 0,
        "column_count": 0,
        "has_tenor_column": False,
        "tenor_count": 0,
        "missing_expected_tenors": "",
        "non_null_numeric_count": 0,
        "status": "missing",
        "error": "",
    }

    if not result["exists"]:
        return result

    result["file_size_bytes"] = os.path.getsize(path)
    if result["file_size_bytes"] == 0:
        result["status"] = "empty_file"
        return result

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        result["status"] = "unreadable_csv"
        result["error"] = repr(exc)
        return result

    result["readable"] = True
    result["row_count"] = len(df)
    result["column_count"] = len(df.columns)
    result["has_tenor_column"] = "Tenor" in df.columns

    if not result["has_tenor_column"]:
        result["status"] = "missing_tenor_column"
        return result

    tenors_present = [str(value) for value in df["Tenor"].dropna().tolist()]
    result["tenor_count"] = len(tenors_present)
    missing_tenors = [tenor for tenor in EXPECTED_TENORS if tenor not in tenors_present]
    result["missing_expected_tenors"] = ",".join(missing_tenors)

    numeric_df = df.drop(columns=["Tenor"], errors="ignore").apply(pd.to_numeric, errors="coerce")
    result["non_null_numeric_count"] = int(numeric_df.notna().sum().sum())

    if result["row_count"] == 0:
        result["status"] = "no_rows"
    elif result["non_null_numeric_count"] == 0:
        result["status"] = "all_numeric_null"
    elif missing_tenors:
        result["status"] = "missing_tenors"
    else:
        result["status"] = "ok"

    return result


def write_csv(path: str, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()
    tickers = [item.strip() for item in args.tickers.split(",") if item.strip()]
    dates = business_dates(args.start_date, args.end_date)

    os.makedirs(args.audit_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    audit_rows = []
    missing_rows = []
    bad_rows = []

    expected_items = [
        (ticker, date_str, sf, cp, metric)
        for ticker in tickers
        for date_str in dates
        for sf in SF_OPTIONS
        for cp in CP_OPTIONS
        for metric in METRIC_OPTIONS
    ]

    for ticker, date_str, sf, cp, metric in progress_iter(
        expected_items, total=len(expected_items), desc="Verify option files"
    ):
        filename = expected_filename(ticker, sf, cp, metric, date_str)
        path = os.path.join(args.raw_path, filename)
        validation = validate_file(path)
        row = {
            "ticker": ticker,
            "date": date_str,
            "sf": sf,
            "cp": cp,
            "metric": metric,
            "filename": filename,
            "path": path,
            **validation,
        }
        audit_rows.append(row)
        if row["status"] == "missing":
            missing_rows.append(row)
        elif row["status"] != "ok":
            bad_rows.append(row)

    summary_counter = Counter(row["status"] for row in audit_rows)
    summary_rows = [
        {"metric": "start_date", "value": args.start_date},
        {"metric": "end_date", "value": args.end_date},
        {"metric": "ticker_count", "value": len(tickers)},
        {"metric": "business_date_count", "value": len(dates)},
        {"metric": "expected_file_count", "value": len(audit_rows)},
        {"metric": "ok_file_count", "value": summary_counter.get("ok", 0)},
        {"metric": "missing_file_count", "value": summary_counter.get("missing", 0)},
        {"metric": "bad_file_count", "value": len(bad_rows)},
    ]
    for status, count in sorted(summary_counter.items()):
        summary_rows.append({"metric": f"status_{status}", "value": count})

    summary_path = os.path.join(args.audit_dir, f"fetch_audit_summary_{timestamp}.csv")
    missing_path = os.path.join(args.audit_dir, f"missing_files_{timestamp}.csv")
    bad_path = os.path.join(args.audit_dir, f"bad_files_{timestamp}.csv")
    full_audit_path = os.path.join(args.audit_dir, f"full_audit_{timestamp}.csv")

    write_csv(summary_path, summary_rows, ["metric", "value"])
    if missing_rows:
        write_csv(missing_path, missing_rows, list(missing_rows[0].keys()))
    else:
        write_csv(missing_path, [], ["ticker", "date", "sf", "cp", "metric", "filename", "path", "exists", "readable", "file_size_bytes", "row_count", "column_count", "has_tenor_column", "tenor_count", "missing_expected_tenors", "non_null_numeric_count", "status", "error"])
    if bad_rows:
        write_csv(bad_path, bad_rows, list(bad_rows[0].keys()))
    else:
        write_csv(bad_path, [], ["ticker", "date", "sf", "cp", "metric", "filename", "path", "exists", "readable", "file_size_bytes", "row_count", "column_count", "has_tenor_column", "tenor_count", "missing_expected_tenors", "non_null_numeric_count", "status", "error"])
    write_csv(full_audit_path, audit_rows, list(audit_rows[0].keys()) if audit_rows else ["ticker", "date", "sf", "cp", "metric", "filename", "path", "exists", "readable", "file_size_bytes", "row_count", "column_count", "has_tenor_column", "tenor_count", "missing_expected_tenors", "non_null_numeric_count", "status", "error"])

    print(f"Audit complete: {summary_path}")
    print(f"Full audit: {full_audit_path}")
    print(f"Missing files: {missing_path}")
    print(f"Bad files: {bad_path}")
    print("Summary:")
    for row in summary_rows:
        print(f"  {row['metric']}: {row['value']}")


if __name__ == "__main__":
    main()
