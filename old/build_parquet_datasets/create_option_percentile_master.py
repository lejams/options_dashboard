import argparse
import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app import DATA_ROOT, tickers


LOOKBACK_DAYS = 365 * 2


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build percentile master parquet files from option master parquet files."
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker list. Defaults to app.py tickers.",
    )
    parser.add_argument(
        "--master-dir",
        default=os.path.join(DATA_ROOT, "master"),
        help="Directory containing <ticker>_master.parquet files.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(DATA_ROOT, "percentile_master"),
        help="Directory for <ticker>_percentile_master.parquet outputs.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files.",
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


def build_percentile_master_for_ticker(master_path: str) -> pd.DataFrame:
    df = read_parquet_safe(master_path)
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["strike_pct"] = pd.to_numeric(df["strike_pct"], errors="coerce")
    df["price_percent"] = pd.to_numeric(df["price_percent"], errors="coerce")
    df["vol"] = pd.to_numeric(df["vol"], errors="coerce")

    df = df.dropna(subset=["date", "surface_type", "option_type", "tenor", "strike_pct"])
    df = df.sort_values(
        ["surface_type", "option_type", "tenor", "strike_pct", "date"]
    ).reset_index(drop=True)

    group_cols = ["surface_type", "option_type", "tenor", "strike_pct"]

    parts = []
    for _, group in df.groupby(group_cols, dropna=False):
        group = group.sort_values("date").copy()
        group["percentile_2y"] = rolling_percentile_prior(
            values=group["price_percent"],
            dates=group["date"],
            lookback_days=LOOKBACK_DAYS,
        )
        parts.append(group)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    if out.empty:
        return out

    final_cols = [
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

    out = out[final_cols].sort_values(
        ["date", "surface_type", "option_type", "tenor", "strike_pct"]
    ).reset_index(drop=True)

    return out


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [x.strip() for x in args.tickers.split(",") if x.strip()]

    for ticker in selected_tickers:
        master_path = os.path.join(args.master_dir, f"{ticker}_master.parquet")
        output_path = os.path.join(args.output_dir, f"{ticker}_percentile_master.parquet")

        if not os.path.exists(master_path):
            print(f"[INFO] Missing master parquet for {ticker}: {master_path}", flush=True)
            continue

        if os.path.exists(output_path) and not args.overwrite:
            print(f"[INFO] Skipping {ticker}, output exists and --overwrite not set", flush=True)
            continue

        print(f"[RUN] Building percentile master for {ticker}", flush=True)
        df_out = build_percentile_master_for_ticker(master_path)
        write_parquet_safe(df_out, output_path)

        latest_date = df_out["date"].max() if not df_out.empty else None
        print(
            f"[OK] {ticker}: rows={len(df_out)}, latest_date={latest_date}, saved={output_path}",
            flush=True,
        )


if __name__ == "__main__":
    main()
