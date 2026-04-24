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
        description="Sync strategies master parquet files from master parquet files."
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


def get_strategies_master_dir():
    return os.path.join(DATA_ROOT, "strategies_master")


def get_reports_dir():
    return os.path.join(DATA_ROOT, "reports")


def rolling_percentile_prior(series: pd.Series, lookback_days: int = LOOKBACK_DAYS) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    dates = pd.to_datetime(series.index, errors="coerce")

    vals = values.to_numpy(dtype=float)
    dts = dates.to_numpy(dtype="datetime64[ns]")
    out = np.full(len(vals), np.nan, dtype=float)

    for i in range(len(vals)):
        if not np.isfinite(vals[i]):
            continue

        cutoff = dts[i] - np.timedelta64(lookback_days, "D")
        mask = (dts >= cutoff) & (dts < dts[i]) & np.isfinite(vals)
        hist = vals[mask]

        if hist.size > 0:
            out[i] = 100.0 * np.mean(hist < vals[i])

    return pd.Series(out, index=series.index)


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

    df = df.dropna(
        subset=["date", "surface_type", "option_type", "tenor", "strike_pct", "price_percent"]
    ).sort_values("date").reset_index(drop=True)

    return df


def build_node_cache(df_master: pd.DataFrame):
    cache = {}

    grouped = df_master.groupby(
        ["surface_type", "option_type", "tenor", "strike_pct"],
        dropna=False,
    )

    for key, group in grouped:
        s = (
            group[["date", "price_percent"]]
            .drop_duplicates("date")
            .set_index("date")["price_percent"]
            .sort_index()
        )
        cache[key] = s

    return cache


def get_series(cache, surface_type: str, option_type: str, tenor: str, strike: float) -> pd.Series:
    return cache.get((surface_type, option_type, tenor, float(strike)), pd.Series(dtype=float))


def combo_to_label(combo):
    parts = []
    for tenor, strike, weight, option_type in combo:
        parts.append(f"{weight} {option_type} {tenor} {strike}")
    return " | ".join(parts)


def compute_two_leg_combo(cache, surface_type, combo):
    (tenor1, strike1, weight1, type1), (tenor2, strike2, weight2, type2) = combo

    s1 = get_series(cache, surface_type, type1, tenor1, strike1)
    s2 = get_series(cache, surface_type, type2, tenor2, strike2)

    if s1.empty or s2.empty:
        return pd.Series(dtype=float)

    df = pd.concat(
        [s1.rename("leg1"), s2.rename("leg2")],
        axis=1,
        sort=True,
    ).dropna()

    if df.empty:
        return pd.Series(dtype=float)

    # Keep legacy strategy convention
    return (df["leg1"] * weight1) - (df["leg2"] * weight2)


def compute_four_leg_combo(cache, surface_type, combo):
    weighted_legs = []

    for idx, (tenor, strike, weight, option_type) in enumerate(combo, start=1):
        s = get_series(cache, surface_type, option_type, tenor, strike)
        if s.empty:
            return pd.Series(dtype=float)
        weighted_legs.append(s.rename(f"leg{idx}") * weight)

    df = pd.concat(weighted_legs, axis=1, sort=True).dropna()

    if df.empty:
        return pd.Series(dtype=float)

    return df.sum(axis=1)


def build_strategy_rows(underlyer: str, strategy_family: str, surface_type: str, combo, combo_series: pd.Series):
    if combo_series.empty:
        return pd.DataFrame()

    combo_series = combo_series.sort_index()
    percentile = rolling_percentile_prior(combo_series)

    out = pd.DataFrame(
        {
            "date": combo_series.index,
            "underlyer": underlyer,
            "surface_type": surface_type,
            "strategy_family": strategy_family,
            "combination_label": combo_to_label(combo),
            "combination_json": json.dumps(combo),
            "price": combo_series.values,
            "percentile_2y": percentile.values,
        }
    )

    return out


def get_strategy_definitions():
    tenors = ["1w", "2w", "3w", "1m", "3m", "6m", "1y"]

    skews = [
        (("1w", 99.0, 1, "Put"), ("1w", 101.0, 1, "Call")),
        (("2w", 98.5, 1, "Put"), ("2w", 101.5, 1, "Call")),
        (("3w", 97.5, 1, "Put"), ("3w", 102.5, 1, "Call")),
        (("1m", 97.0, 1, "Put"), ("1m", 103.0, 1, "Call")),
        (("3m", 96.0, 1, "Put"), ("3m", 104.0, 1, "Call")),
        (("6m", 94.0, 1, "Put"), ("6m", 106.0, 1, "Call")),
        (("1y", 90.0, 1, "Put"), ("1y", 110.0, 1, "Call")),
    ]

    straddles = [[(tenor, 100.0, 1, "Call"), (tenor, 100.0, -1, "Put")] for tenor in tenors]

    strangles = [
        (("1w", 98.5, 1, "Put"), ("1w", 101.5, -1, "Call")),
        (("2w", 98.0, 1, "Put"), ("2w", 102.0, -1, "Call")),
        (("3w", 97.5, 1, "Put"), ("3w", 102.5, -1, "Call")),
        (("1m", 97.0, 1, "Put"), ("1m", 103.0, -1, "Call")),
        (("3m", 95.0, 1, "Put"), ("3m", 105.0, -1, "Call")),
        (("6m", 92.0, 1, "Put"), ("6m", 108.0, -1, "Call")),
        (("1y", 88.0, 1, "Put"), ("1y", 112.0, -1, "Call")),
    ]

    call_spreads = [
        (("1w", 101.0, 1, "Call"), ("1w", 102.0, 1, "Call")),
        (("2w", 101.5, 1, "Call"), ("2w", 102.5, 1, "Call")),
        (("3w", 102.0, 1, "Call"), ("3w", 103.0, 1, "Call")),
        (("1m", 103.0, 1, "Call"), ("1m", 105.0, 1, "Call")),
        (("3m", 104.0, 1, "Call"), ("3m", 107.0, 1, "Call")),
        (("6m", 106.0, 1, "Call"), ("6m", 110.0, 1, "Call")),
        (("1y", 108.0, 1, "Call"), ("1y", 114.0, 1, "Call")),
    ]

    put_spreads = [
        (("1w", 99.0, 1, "Put"), ("1w", 98.0, 1, "Put")),
        (("2w", 98.5, 1, "Put"), ("2w", 97.5, 1, "Put")),
        (("3w", 98.0, 1, "Put"), ("3w", 97.0, 1, "Put")),
        (("1m", 97.0, 1, "Put"), ("1m", 95.0, 1, "Put")),
        (("3m", 96.0, 1, "Put"), ("3m", 93.0, 1, "Put")),
        (("6m", 94.0, 1, "Put"), ("6m", 90.0, 1, "Put")),
        (("1y", 92.0, 1, "Put"), ("1y", 86.0, 1, "Put")),
    ]

    call_ratios = [
        (("1w", 101.0, 1, "Call"), ("1w", 102.0, 2, "Call")),
        (("2w", 101.5, 1, "Call"), ("2w", 102.5, 2, "Call")),
        (("3w", 102.0, 1, "Call"), ("3w", 103.0, 2, "Call")),
        (("1m", 103.0, 1, "Call"), ("1m", 105.0, 2, "Call")),
        (("3m", 104.0, 1, "Call"), ("3m", 107.0, 2, "Call")),
        (("6m", 106.0, 1, "Call"), ("6m", 110.0, 2, "Call")),
        (("1y", 108.0, 1, "Call"), ("1y", 114.0, 2, "Call")),
    ]

    put_ratios = [
        (("1w", 99.0, 1, "Put"), ("1w", 98.0, 2, "Put")),
        (("2w", 98.5, 1, "Put"), ("2w", 97.5, 2, "Put")),
        (("3w", 98.0, 1, "Put"), ("3w", 97.0, 2, "Put")),
        (("1m", 97.0, 1, "Put"), ("1m", 95.0, 2, "Put")),
        (("3m", 96.0, 1, "Put"), ("3m", 93.0, 2, "Put")),
        (("6m", 94.0, 1, "Put"), ("6m", 90.0, 2, "Put")),
        (("1y", 92.0, 1, "Put"), ("1y", 86.0, 2, "Put")),
    ]

    call_calendars = [
        (("1w", 101.0, -1, "Call"), ("1m", 101.0, -1, "Call")),
        (("1w", 101.0, -1, "Call"), ("2m", 101.0, -1, "Call")),
        (("1w", 101.0, -1, "Call"), ("3m", 101.0, -1, "Call")),
        (("2w", 102.0, -1, "Call"), ("1m", 102.0, -1, "Call")),
        (("2w", 102.0, -1, "Call"), ("2m", 102.0, -1, "Call")),
        (("2w", 102.0, -1, "Call"), ("3m", 102.0, -1, "Call")),
        (("1m", 103.0, -1, "Call"), ("3m", 103.0, -1, "Call")),
        (("1m", 104.0, -1, "Call"), ("6m", 104.0, -1, "Call")),
        (("3m", 106.0, -1, "Call"), ("6m", 106.0, -1, "Call")),
        (("3m", 106.0, -1, "Call"), ("1y", 106.0, -1, "Call")),
    ]

    put_calendars = [
        (("1w", 99.0, -1, "Put"), ("1m", 99.0, -1, "Put")),
        (("1w", 99.0, -1, "Put"), ("2m", 99.0, -1, "Put")),
        (("1w", 99.0, -1, "Put"), ("3m", 99.0, -1, "Put")),
        (("2w", 98.0, -1, "Put"), ("1m", 98.0, -1, "Put")),
        (("2w", 98.0, -1, "Put"), ("2m", 98.0, -1, "Put")),
        (("2w", 98.0, -1, "Put"), ("3m", 98.0, -1, "Put")),
        (("1m", 97.0, -1, "Put"), ("3m", 97.0, -1, "Put")),
        (("1m", 96.0, -1, "Put"), ("6m", 96.0, -1, "Put")),
        (("3m", 94.0, -1, "Put"), ("6m", 94.0, -1, "Put")),
        (("3m", 94.0, -1, "Put"), ("1y", 94.0, -1, "Put")),
    ]

    iron_condors = [
        (("1w", 99.0, 1, "Put"), ("1w", 98.5, -1, "Put"), ("1w", 101.0, 1, "Call"), ("1w", 101.5, -1, "Call")),
        (("2w", 98.5, 1, "Put"), ("2w", 98.0, -1, "Put"), ("2w", 101.5, 1, "Call"), ("2w", 102.0, -1, "Call")),
        (("3w", 98.0, 1, "Put"), ("3w", 97.0, -1, "Put"), ("3w", 102.0, 1, "Call"), ("3w", 103.0, -1, "Call")),
        (("1m", 97.0, 1, "Put"), ("1m", 95.0, -1, "Put"), ("1m", 103.0, 1, "Call"), ("1m", 105.0, -1, "Call")),
        (("3m", 96.0, 1, "Put"), ("3m", 94.0, -1, "Put"), ("3m", 104.0, 1, "Call"), ("3m", 106.0, -1, "Call")),
        (("6m", 92.0, 1, "Put"), ("6m", 90.0, -1, "Put"), ("6m", 108.0, 1, "Call"), ("6m", 110.0, -1, "Call")),
    ]

    iron_butterflies = [
        (("1w", 100.0, 1, "Put"), ("1w", 100.0, 1, "Call"), ("1w", 98.5, -1, "Put"), ("1w", 101.5, -1, "Call")),
        (("2w", 100.0, 1, "Put"), ("2w", 100.0, 1, "Call"), ("2w", 98.0, -1, "Put"), ("2w", 102.0, -1, "Call")),
        (("3w", 100.0, 1, "Put"), ("3w", 100.0, 1, "Call"), ("3w", 97.5, -1, "Put"), ("3w", 102.5, -1, "Call")),
        (("1m", 100.0, 1, "Put"), ("1m", 100.0, 1, "Call"), ("1m", 97.0, -1, "Put"), ("1m", 103.0, -1, "Call")),
        (("3m", 100.0, 1, "Put"), ("3m", 100.0, 1, "Call"), ("3m", 95.0, -1, "Put"), ("3m", 105.0, -1, "Call")),
        (("6m", 100.0, 1, "Put"), ("6m", 100.0, 1, "Call"), ("6m", 92.0, -1, "Put"), ("6m", 108.0, -1, "Call")),
    ]

    return {
        "skews": ("fwd", skews),
        "straddles": ("fwd", straddles),
        "strangles": ("fwd", strangles),
        "call_spreads": ("fwd", call_spreads),
        "put_spreads": ("fwd", put_spreads),
        "call_ratios": ("fwd", call_ratios),
        "put_ratios": ("fwd", put_ratios),
        "call_calendars": ("spot", call_calendars),
        "put_calendars": ("spot", put_calendars),
        "iron_condors": ("fwd", iron_condors),
        "iron_butterflies": ("fwd", iron_butterflies),
    }


def build_strategies_master_for_ticker(df_master: pd.DataFrame, ticker: str, start_date=None, end_date=None):
    if df_master.empty:
        return pd.DataFrame(), {
            "rows_input": 0,
            "rows_output": 0,
            "latest_date": None,
            "strategies_count": 0,
        }

    cache = build_node_cache(df_master)
    strategy_defs = get_strategy_definitions()

    outputs = []
    strategies_count = 0

    for family, (surface_type, combos) in strategy_defs.items():
        for combo in combos:
            if len(combo) == 2:
                combo_series = compute_two_leg_combo(cache, surface_type, combo)
            else:
                combo_series = compute_four_leg_combo(cache, surface_type, combo)

            rows = build_strategy_rows(
                underlyer=ticker,
                strategy_family=family,
                surface_type=surface_type,
                combo=combo,
                combo_series=combo_series,
            )

            if not rows.empty:
                outputs.append(rows)
                strategies_count += 1

    if not outputs:
        return pd.DataFrame(), {
            "rows_input": len(df_master),
            "rows_output": 0,
            "latest_date": None,
            "strategies_count": 0,
        }

    result_df = pd.concat(outputs, ignore_index=True)

    if start_date is not None:
        result_df = result_df[result_df["date"] >= start_date].copy()
    if end_date is not None:
        result_df = result_df[result_df["date"] <= end_date].copy()

    result_df = result_df.sort_values(
        ["date", "strategy_family", "combination_label"]
    ).reset_index(drop=True)

    latest_date = result_df["date"].max() if not result_df.empty else None

    stats = {
        "rows_input": len(df_master),
        "rows_output": len(result_df),
        "latest_date": latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else None,
        "strategies_count": strategies_count,
    }

    return result_df, stats


def save_report(report_payload, reports_dir, run_label):
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, f"sync_strategies_master_{run_label}.json")
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
    strategies_master_dir = get_strategies_master_dir()
    reports_dir = get_reports_dir()

    os.makedirs(strategies_master_dir, exist_ok=True)

    run_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")

    ticker_reports = []
    updated = 0

    for ticker in selected_tickers:
        master_path = os.path.join(master_dir, f"{ticker}_master.parquet")
        output_path = os.path.join(strategies_master_dir, f"{ticker}_strategies_master.parquet")

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
                    "strategies_count": 0,
                    "errors": ["master parquet not found"],
                }
            )
            continue

        try:
            df_master = load_master_for_ticker(master_dir, ticker)
            df_out, stats = build_strategies_master_for_ticker(
                df_master,
                ticker,
                start_date=start_date,
                end_date=end_date,
            )

            write_parquet_safe(df_out, output_path)
            updated += 1

            ticker_reports.append(
                {
                    "ticker": ticker,
                    "status": "updated",
                    "master_path": master_path,
                    "output_path": output_path,
                    "rows_input": stats["rows_input"],
                    "rows_output": stats["rows_output"],
                    "latest_date": stats["latest_date"],
                    "strategies_count": stats["strategies_count"],
                    "errors": [],
                }
            )

            print(
                f"[OK] {ticker}: rows_input={stats['rows_input']} rows_output={stats['rows_output']} "
                f"strategies={stats['strategies_count']} latest_date={stats['latest_date']}",
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
                    "strategies_count": 0,
                    "errors": [str(exc)],
                }
            )
            print(f"[FAIL] {ticker}: {exc}", flush=True)

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
            "strategies_master_dir": strategies_master_dir,
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

    print(f"\nStrategies sync status: {status}", flush=True)
    print(f"Tickers updated: {updated}", flush=True)
    print(f"Tickers failed: {failed}", flush=True)
    print(f"Tickers missing master: {missing_master}", flush=True)
    print(f"Report JSON: {report_path}", flush=True)


if __name__ == "__main__":
    main()
