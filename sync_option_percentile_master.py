import argparse
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app import DATA_ROOT, env, tickers


LOOKBACK_DAYS = 365 * 2


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sync percentile master parquet files from master parquet files."
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker list. Defaults to app.py tickers.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date filter in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date filter in YYYY-MM-DD format.",
    )
    return parser.parse_args()


def read_parquet_safe(path: str) -> pd.DataFrame:
    table = pq.read_table(path)
    return table.to_pandas()


def write_parquet_safe(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = df.reset_index(drop=True).copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)


def get_master_dir():
    return os.path.join(DATA_ROOT, "master")


def get_percentile_master_dir():
    return os.path.join(DATA_ROOT, "percentile_master")


def get_reports_dir():
    return os.path.join(DATA_ROOT, "reports")


def rolling_percentile_prior(values: pd.Series, dates: pd.Series, lookback_days: int = LOOKBACK_DAYS) -> pd.Series:
    vals = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    dts = pd.to_datetime(dates, errors="coerce").to_numpy(dtype="datetime64[ns]")

    out = np.full(len(vals), np.nan, dtype=float)

    for i in range(len(vals)):
        if not np.isfinite(vals[i]):
            continue

        cutoff = dts[i] - np.timedelta64(lookback_days, "D")
        mask = (dts >= cutoff) & (dts < dts[i]) & np.isfinite(vals)
        hist = vals[mask]

        if hist.size > 0:
            out[i] = 100.0 * np.mean(hist < vals[i])

    return pd.Series(out, index=values.index)


def load_master_for_ticker(master_dir: str, ticker: str) -> pd.DataFrame:
    path = os.path.join(master_dir, f"{ticker}_master.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()

    df = read_parquet_safe(path)
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["strike_pct"] = pd.to_numeric(df["strike_pct"], errors="coerce")
    df["price_percent"] = pd.to_numeric(df["price_percent"], errors="coerce")
    df["vol"] = pd.to_numeric(df["vol"], errors="coerce")

    df = df.dropna(
        subset=["date", "surface_type", "option_type", "tenor", "strike_pct"]
    ).sort_values(
        ["surface_type", "option_type", "tenor", "strike_pct", "date"]
    ).reset_index(drop=True)

    return df


def build_percentile_master_for_ticker(df_master: pd.DataFrame, start_date=None, end_date=None):
    if df_master.empty:
        return pd.DataFrame(), {
            "rows_input": 0,
            "rows_output": 0,
            "latest_date": None,
            "groups_processed": 0,
        }

    source_df = df_master.copy()

    group_cols = ["surface_type", "option_type", "tenor", "strike_pct"]
    outputs = []
    groups_processed = 0

    for _, group in source_df.groupby(group_cols, dropna=False):
        group = group.sort_values("date").copy()
        group["percentile_2y"] = rolling_percentile_prior(
            values=group["price_percent"],
            dates=group["date"],
            lookback_days=LOOKBACK_DAYS,
        )
        outputs.append(group)
        groups_processed += 1

    result_df = pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame()

    if result_df.empty:
        return result_df, {
            "rows_input": len(df_master),
            "rows_output": 0,
            "latest_date": None,
            "groups_processed": groups_processed,
        }

    if start_date is not None:
        result_df = result_df[result_df["date"] >= start_date].copy()
    if end_date is not None:
        result_df = result_df[result_df["date"] <= end_date].copy()

    result_df = result_df[
        [
            "date",
            "underlyer",
            "surface_type",
            "option_type",
            "tenor",
            "strike_pct",
            "price_percent",
            "vol",
            "percentile_2y",
        ]
    ].sort_values(
        ["date", "surface_type", "option_type", "tenor", "strike_pct"]
    ).reset_index(drop=True)

    latest_date = result_df["date"].max() if not result_df.empty else None

    stats = {
        "rows_input": len(df_master),
        "rows_output": len(result_df),
        "latest_date": latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else None,
        "groups_processed": groups_processed,
    }

    return result_df, stats


def save_report(report_payload, reports_dir, run_label):
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, f"sync_percentile_master_{run_label}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2)
    return path


def main():
    args = parse_args()

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [x.strip() for x in args.tickers.split(",") if x.strip()]

    start_date = pd.to_datetime(args.start_date).normalize() if args.start_date else None
    end_date = pd.to_datetime(args.end_date).normalize() if args.end_date else None

    master_dir = get_master_dir()
    percentile_master_dir = get_percentile_master_dir()
    reports_dir = get_reports_dir()

    os.makedirs(percentile_master_dir, exist_ok=True)

    run_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")

    ticker_reports = []
    tickers_updated = 0

    for ticker in selected_tickers:
        master_path = os.path.join(master_dir, f"{ticker}_master.parquet")
        output_path = os.path.join(percentile_master_dir, f"{ticker}_percentile_master.parquet")

        if not os.path.exists(master_path):
            ticker_reports.append(
                {
                    "ticker": ticker,
                    "status": "missing_master",
                    "master_path": master_path,
                    "output_path": output_path,
                    "rows_input": 0,
                    "rows_output": 0,
                    "latest_date": None,
                    "groups_processed": 0,
                    "errors": ["master parquet not found"],
                }
            )
            continue

        try:
            df_master = load_master_for_ticker(master_dir, ticker)
            df_out, stats = build_percentile_master_for_ticker(
                df_master,
                start_date=start_date,
                end_date=end_date,
            )

            write_parquet_safe(df_out, output_path)
            tickers_updated += 1

            ticker_reports.append(
                {
                    "ticker": ticker,
                    "status": "updated",
                    "master_path": master_path,
                    "output_path": output_path,
                    "rows_input": stats["rows_input"],
                    "rows_output": stats["rows_output"],
                    "latest_date": stats["latest_date"],
                    "groups_processed": stats["groups_processed"],
                    "errors": [],
                }
            )

            print(
                f"[OK] {ticker}: rows_input={stats['rows_input']} rows_output={stats['rows_output']} "
                f"groups={stats['groups_processed']} latest_date={stats['latest_date']}",
                flush=True,
            )

        except Exception as exc:
            ticker_reports.append(
                {
                    "ticker": ticker,
                    "status": "failed",
                    "master_path": master_path,
                    "output_path": output_path,
                    "rows_input": 0,
                    "rows_output": 0,
                    "latest_date": None,
                    "groups_processed": 0,
                    "errors": [str(exc)],
                }
            )
            print(f"[FAIL] {ticker}: {exc}", flush=True)

    updated = sum(1 for x in ticker_reports if x["status"] == "updated")
    failed = sum(1 for x in ticker_reports if x["status"] == "failed")
    missing_master = sum(1 for x in ticker_reports if x["status"] == "missing_master")

    if failed == 0 and missing_master == 0:
        status = "OK"
    elif updated > 0:
        status = "PARTIAL_SUCCESS"
    else:
        status = "FAILED"

    report_payload = {
        "run": {
            "run_label": run_label,
            "started_at": started_at,
            "env": env,
            "master_dir": master_dir,
            "percentile_master_dir": percentile_master_dir,
            "tickers_requested": selected_tickers,
            "start_date": start_date.strftime("%Y-%m-%d") if start_date is not None else None,
            "end_date": end_date.strftime("%Y-%m-%d") if end_date is not None else None,
        },
        "summary": {
            "tickers_processed": len(selected_tickers),
            "tickers_updated": updated,
            "tickers_failed": failed,
            "tickers_missing_master": missing_master,
            "status": status,
        },
        "tickers": ticker_reports,
    }

    report_path = save_report(report_payload, reports_dir, run_label)

    print(f"\nPercentile sync status: {status}", flush=True)
    print(f"Tickers updated: {updated}", flush=True)
    print(f"Tickers failed: {failed}", flush=True)
    print(f"Tickers missing master: {missing_master}", flush=True)
    print(f"Report JSON: {report_path}", flush=True)


if __name__ == "__main__":
    main()
