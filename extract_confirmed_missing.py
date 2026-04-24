import argparse
import csv
import os
from collections import Counter, defaultdict
from datetime import datetime

import pandas as pd

DEFAULT_AUDIT_DIR = "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/audit"
DEFAULT_REVALIDATED = os.path.join(DEFAULT_AUDIT_DIR, "problematic_post_backfill_revalidated.csv")


def parse_args():
    parser = argparse.ArgumentParser(description="Extract and summarize confirmed missing option files.")
    parser.add_argument("--revalidated-path", default=DEFAULT_REVALIDATED)
    parser.add_argument(
        "--output-prefix",
        default=datetime.now().strftime("confirmed_missing_%Y%m%d_%H%M%S"),
        help="Prefix for generated CSV files in the audit directory.",
    )
    parser.add_argument("--audit-dir", default=DEFAULT_AUDIT_DIR)
    return parser.parse_args()


def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()
    df = pd.read_csv(args.revalidated_path)
    missing_df = df[df["revalidation_class"] == "confirmed_missing"].copy()
    if missing_df.empty:
        raise SystemExit("No confirmed_missing rows found.")

    missing_df = missing_df.sort_values(["ticker", "date", "sf", "cp", "metric"])

    detailed_path = os.path.join(args.audit_dir, f"{args.output_prefix}_detailed.csv")
    by_ticker_date_path = os.path.join(args.audit_dir, f"{args.output_prefix}_by_ticker_date.csv")
    by_ticker_path = os.path.join(args.audit_dir, f"{args.output_prefix}_by_ticker.csv")
    by_type_path = os.path.join(args.audit_dir, f"{args.output_prefix}_by_type.csv")

    detailed_rows = missing_df.to_dict(orient="records")
    write_csv(detailed_path, detailed_rows, list(missing_df.columns))

    grouped_td = (
        missing_df.groupby(["ticker", "date"])
        .agg(
            missing_count=("path", "size"),
            sf_values=("sf", lambda s: ",".join(sorted(set(s)))),
            cp_values=("cp", lambda s: ",".join(sorted(set(s)))),
            metric_values=("metric", lambda s: ",".join(sorted(set(s)))),
            sample_paths=("path", lambda s: "|".join(list(s.head(6)))),
        )
        .reset_index()
        .sort_values(["missing_count", "ticker", "date"], ascending=[False, True, True])
    )
    write_csv(by_ticker_date_path, grouped_td.to_dict(orient="records"), list(grouped_td.columns))

    grouped_ticker = (
        missing_df.groupby(["ticker"])
        .agg(
            missing_count=("path", "size"),
            unique_dates=("date", "nunique"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            sf_values=("sf", lambda s: ",".join(sorted(set(s)))),
            cp_values=("cp", lambda s: ",".join(sorted(set(s)))),
            metric_values=("metric", lambda s: ",".join(sorted(set(s)))),
        )
        .reset_index()
        .sort_values(["missing_count", "ticker"], ascending=[False, True])
    )
    write_csv(by_ticker_path, grouped_ticker.to_dict(orient="records"), list(grouped_ticker.columns))

    grouped_type = (
        missing_df.groupby(["sf", "cp", "metric"])
        .agg(
            missing_count=("path", "size"),
            unique_tickers=("ticker", "nunique"),
            unique_dates=("date", "nunique"),
        )
        .reset_index()
        .sort_values(["missing_count", "sf", "cp", "metric"], ascending=[False, True, True, True])
    )
    write_csv(by_type_path, grouped_type.to_dict(orient="records"), list(grouped_type.columns))

    print(f"Detailed confirmed_missing: {detailed_path}")
    print(f"By ticker/date: {by_ticker_date_path}")
    print(f"By ticker: {by_ticker_path}")
    print(f"By type: {by_type_path}")
    print("Top confirmed_missing tickers:")
    for row in grouped_ticker.head(10).to_dict(orient="records"):
        print(f"  {row['ticker']}: {row['missing_count']} files across {row['unique_dates']} date(s)")


if __name__ == "__main__":
    main()
