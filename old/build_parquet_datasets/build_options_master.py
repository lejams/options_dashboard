import argparse
import os
import re
from datetime import datetime, timezone
datetime.now(timezone.utc)

from typing import Optional

import pandas as pd

from app import DATA_ROOT, env, tickers


RAW_FILENAME_PATTERN = re.compile(
    r"^(?P<underlyer>.+)_(?P<surface_type>spot|fwd)_(?P<option_type>Call|Put)_option_(?P<metric>percent|vol)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build master option dataset from raw_percent CSV files."
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
    parser.add_argument(
        "--raw-dir",
        default=None,
        help="Optional override for raw_percent directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional override for master output directory.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing parquet files instead of merging with them.",
    )
    return parser.parse_args()


def resolve_directories(raw_dir_arg: Optional[str], output_dir_arg: Optional[str]):
    if raw_dir_arg:
        raw_dir = raw_dir_arg
    else:
        raw_dir = os.path.join(DATA_ROOT, "raw_percent")

    if output_dir_arg:
        output_dir = output_dir_arg
    else:
        output_dir = os.path.join(DATA_ROOT, "master")

    os.makedirs(output_dir, exist_ok=True)
    return raw_dir, output_dir


def parse_raw_filename(filename: str):
    match = RAW_FILENAME_PATTERN.match(filename)
    if not match:
        return None

    info = match.groupdict()
    info["date"] = pd.to_datetime(info["date"]).normalize()
    return info


def normalize_strike_column(col_name):
    """
    Converts strike column names like '100.0' or 100.0 to float.
    Returns None if the column is not a valid strike.
    """
    try:
        return float(str(col_name).strip())
    except (TypeError, ValueError):
        return None


def load_surface_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Tenor" not in df.columns:
        raise ValueError(f"'Tenor' column not found in {path}")

    df = df.copy()
    df["Tenor"] = df["Tenor"].astype(str).str.strip()

    valid_strike_cols = {}
    for col in df.columns:
        if col == "Tenor":
            continue
        strike_value = normalize_strike_column(col)
        if strike_value is not None:
            valid_strike_cols[col] = strike_value

    if not valid_strike_cols:
        raise ValueError(f"No valid strike columns found in {path}")

    keep_cols = ["Tenor"] + list(valid_strike_cols.keys())
    df = df[keep_cols]

    renamed_cols = {"Tenor": "tenor"}
    renamed_cols.update(valid_strike_cols)
    df = df.rename(columns=renamed_cols)

    return df


def surface_to_long(df_surface: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    strike_cols = [col for col in df_surface.columns if col != "tenor"]

    df_long = df_surface.melt(
        id_vars="tenor",
        value_vars=strike_cols,
        var_name="strike_pct",
        value_name="metric_value",
    )

    df_long["date"] = metadata["date"]
    df_long["underlyer"] = metadata["underlyer"]
    df_long["surface_type"] = metadata["surface_type"]
    df_long["option_type"] = metadata["option_type"]
    df_long["metric"] = metadata["metric"]

    df_long["strike_pct"] = pd.to_numeric(df_long["strike_pct"], errors="coerce")
    df_long["metric_value"] = pd.to_numeric(df_long["metric_value"], errors="coerce")

    df_long = df_long.dropna(subset=["strike_pct"])
    df_long["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")



    cols = [
        "date",
        "underlyer",
        "surface_type",
        "option_type",
        "tenor",
        "strike_pct",
        "metric",
        "metric_value",
        "loaded_at",
    ]
    return df_long[cols]


def build_metric_long_df(raw_dir: str, selected_tickers, start_date=None, end_date=None) -> pd.DataFrame:
    all_rows = []

    filenames = sorted(os.listdir(raw_dir))
    print(f"Scanning {len(filenames)} files in {raw_dir}", flush=True)

    for filename in filenames:
        metadata = parse_raw_filename(filename)
        if metadata is None:
            continue

        if metadata["underlyer"] not in selected_tickers:
            continue

        if start_date is not None and metadata["date"] < start_date:
            continue

        if end_date is not None and metadata["date"] > end_date:
            continue

        file_path = os.path.join(raw_dir, filename)

        try:
            df_surface = load_surface_csv(file_path)
            df_long = surface_to_long(df_surface, metadata)
            df_long["source_file"] = filename
            all_rows.append(df_long)
        except Exception as exc:
            print(f"[WARN] Skipping {filename}: {exc}", flush=True)

    if not all_rows:
        return pd.DataFrame(
            columns=[
                "date",
                "underlyer",
                "surface_type",
                "option_type",
                "tenor",
                "strike_pct",
                "metric",
                "metric_value",
                "loaded_at",
                "source_file",
            ]
        )

    out = pd.concat(all_rows, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.normalize()
    out["tenor"] = out["tenor"].astype(str)
    out["underlyer"] = out["underlyer"].astype(str)
    out["surface_type"] = out["surface_type"].astype(str)
    out["option_type"] = out["option_type"].astype(str)
    out["metric"] = out["metric"].astype(str)

    return out


def build_asset_master(df_long: pd.DataFrame, underlyer: str) -> pd.DataFrame:
    asset_df = df_long[df_long["underlyer"] == underlyer].copy()
    if asset_df.empty:
        return pd.DataFrame()

    key_cols = ["date", "underlyer", "surface_type", "option_type", "tenor", "strike_pct"]

    percent_df = asset_df[asset_df["metric"] == "percent"].copy()
    percent_df = percent_df.rename(
        columns={
            "metric_value": "price_percent",
            "source_file": "source_price_file",
        }
    ).drop(columns=["metric"])

    vol_df = asset_df[asset_df["metric"] == "vol"].copy()
    vol_df = vol_df.rename(
        columns={
            "metric_value": "vol",
            "source_file": "source_vol_file",
        }
    ).drop(columns=["metric"])

    percent_df = percent_df.drop_duplicates(subset=key_cols, keep="last")
    vol_df = vol_df.drop_duplicates(subset=key_cols, keep="last")

    merged = pd.merge(
        percent_df,
        vol_df[key_cols + ["vol", "source_vol_file"]],
        on=key_cols,
        how="outer",
    )

    merged["currency"] = merged["underlyer"].map(infer_currency)
    merged["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")



    final_cols = [
        "date",
        "underlyer",
        "surface_type",
        "option_type",
        "tenor",
        "strike_pct",
        "price_percent",
        "vol",
        "currency",
        "source_price_file",
        "source_vol_file",
        "loaded_at",
    ]

    merged = merged[final_cols].copy()
    merged["date"] = pd.to_datetime(merged["date"]).dt.normalize()
    merged["strike_pct"] = pd.to_numeric(merged["strike_pct"], errors="coerce")
    merged["price_percent"] = pd.to_numeric(merged["price_percent"], errors="coerce")
    merged["vol"] = pd.to_numeric(merged["vol"], errors="coerce")

    merged = merged.drop_duplicates(
        subset=["date", "underlyer", "surface_type", "option_type", "tenor", "strike_pct"],
        keep="last",
    )
    merged = merged.sort_values(
        by=["date", "surface_type", "option_type", "tenor", "strike_pct"]
    ).reset_index(drop=True)

    return merged


def infer_currency(underlyer: str) -> str:
    if underlyer == "SX5E":
        return "EUR"
    if underlyer == "NKY":
        return "JPY"
    return "USD"


def save_asset_master(df_asset: pd.DataFrame, output_dir: str, underlyer: str, overwrite: bool):
    output_path = os.path.join(output_dir, f"{underlyer}_master.csv")

    df_asset = df_asset.reset_index(drop=True).copy()

    df_asset["date"] = pd.to_datetime(df_asset["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_asset["underlyer"] = df_asset["underlyer"].astype(str)
    df_asset["surface_type"] = df_asset["surface_type"].astype(str)
    df_asset["option_type"] = df_asset["option_type"].astype(str)
    df_asset["tenor"] = df_asset["tenor"].astype(str)
    df_asset["currency"] = df_asset["currency"].astype(str)
    df_asset["source_price_file"] = df_asset["source_price_file"].fillna("").astype(str)
    df_asset["source_vol_file"] = df_asset["source_vol_file"].fillna("").astype(str)
    df_asset["loaded_at"] = df_asset["loaded_at"].astype(str)

    df_asset["strike_pct"] = pd.to_numeric(df_asset["strike_pct"], errors="coerce").astype(float)
    df_asset["price_percent"] = pd.to_numeric(df_asset["price_percent"], errors="coerce").astype(float)
    df_asset["vol"] = pd.to_numeric(df_asset["vol"], errors="coerce").astype(float)

    if os.path.exists(output_path) and not overwrite:
        print(f"[INFO] {underlyer}: file exists, skipping because --overwrite not set", flush=True)
        return output_path, 0, len(df_asset)

    df_asset.to_csv(output_path, index=False)
    return output_path, 0, len(df_asset)


def print_asset_summary(underlyer: str, df_asset: pd.DataFrame, output_path: str):
    if df_asset.empty:
        print(f"[INFO] {underlyer}: no rows to save", flush=True)
        return

    min_date = df_asset["date"].min()
    max_date = df_asset["date"].max()
    n_rows = len(df_asset)
    n_dates = df_asset["date"].nunique()
    n_missing_price = df_asset["price_percent"].isna().sum()
    n_missing_vol = df_asset["vol"].isna().sum()

    print(
        f"[OK] {underlyer}: rows={n_rows}, dates={n_dates}, "
        f"range={min_date.date()} -> {max_date.date()}, "
        f"missing_price={n_missing_price}, missing_vol={n_missing_vol}, "
        f"saved={output_path}",
        flush=True,
    )


def main():
    args = parse_args()

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [item.strip() for item in args.tickers.split(",") if item.strip()]

    start_date = pd.to_datetime(args.start_date).normalize() if args.start_date else None
    end_date = pd.to_datetime(args.end_date).normalize() if args.end_date else None

    raw_dir, output_dir = resolve_directories(args.raw_dir, args.output_dir)

    if not os.path.isdir(raw_dir):
        raise SystemExit(f"Raw directory not found: {raw_dir}")

    print(f"Environment: {env}", flush=True)
    print(f"Raw dir: {raw_dir}", flush=True)
    print(f"Output dir: {output_dir}", flush=True)
    print(f"Tickers: {selected_tickers}", flush=True)
    if start_date is not None:
        print(f"Start date filter: {start_date.date()}", flush=True)
    if end_date is not None:
        print(f"End date filter: {end_date.date()}", flush=True)

    df_long = build_metric_long_df(
        raw_dir=raw_dir,
        selected_tickers=selected_tickers,
        start_date=start_date,
        end_date=end_date,
    )

    if df_long.empty:
        print("[INFO] No matching raw files found. Nothing to build.", flush=True)
        return

    print(f"Built long metric table with {len(df_long)} rows", flush=True)

    for underlyer in selected_tickers:
        df_asset = build_asset_master(df_long, underlyer)
        if df_asset.empty:
            print(f"[INFO] {underlyer}: no usable data found", flush=True)
            continue

        output_path, _, _ = save_asset_master(
            df_asset=df_asset,
            output_dir=output_dir,
            underlyer=underlyer,
            overwrite=args.overwrite,
        )
        print_asset_summary(underlyer, df_asset, output_path)


if __name__ == "__main__":
    main()
