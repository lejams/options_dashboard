import argparse
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app import DATA_ROOT, tickers


TEXT_COLUMNS = [
    "underlyer",
    "surface_type",
    "option_type",
    "tenor",
    "currency",
    "source_price_file",
    "source_vol_file",
    "loaded_at",
]

NUMERIC_COLUMNS = [
    "strike_pct",
    "price_percent",
    "vol",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Convert master CSV files to parquet.")
    parser.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker list. Defaults to app.py tickers.",
    )
    parser.add_argument(
        "--master-dir",
        default=os.path.join(DATA_ROOT, "master"),
        help="Directory containing <ticker>_master.csv files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite parquet files if they already exist.",
    )
    return parser.parse_args()


def load_master_csv(csv_path: str) -> pd.DataFrame:
    dtype_map = {col: "string" for col in TEXT_COLUMNS}

    df = pd.read_csv(
        csv_path,
        parse_dates=["date"],
        dtype=dtype_map,
        low_memory=False,
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in TEXT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string")
        df[col] = df[col].fillna("").astype("string")

    return df.reset_index(drop=True)


def write_parquet(df: pd.DataFrame, parquet_path: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, parquet_path)


def main():
    args = parse_args()

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [x.strip() for x in args.tickers.split(",") if x.strip()]

    for ticker in selected_tickers:
        csv_path = os.path.join(args.master_dir, f"{ticker}_master.csv")
        parquet_path = os.path.join(args.master_dir, f"{ticker}_master.parquet")

        if not os.path.exists(csv_path):
            print(f"[INFO] Missing CSV for {ticker}: {csv_path}", flush=True)
            continue

        if os.path.exists(parquet_path) and not args.overwrite:
            print(f"[INFO] Skipping {ticker}, parquet exists and --overwrite not set", flush=True)
            continue

        print(f"[RUN] Converting {csv_path}", flush=True)
        df = load_master_csv(csv_path)
        write_parquet(df, parquet_path)
        print(f"[OK] Saved {parquet_path} with {len(df)} rows", flush=True)


if __name__ == "__main__":
    main()
