import argparse
import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app import DATA_ROOT, env, tickers


RAW_FILENAME_PATTERN = re.compile(
    r"^(?P<ticker>.+)_(?P<surface>spot|fwd)_(?P<option_type>Call|Put)_option_(?P<metric>percent|vol)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$"
)

KEY_COLS = ["date", "underlyer", "surface_type", "option_type", "tenor", "strike_pct"]


def parse_args():
    parser = argparse.ArgumentParser(description="Sync raw option CSV files into master parquet files.")
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
        "--strict",
        action="store_true",
        help="If any block is invalid for a ticker/date scope, skip syncing that ticker/date scope.",
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


def get_base_data_dir():
    return DATA_ROOT


def get_raw_dir():
    return os.path.join(get_base_data_dir(), "raw_percent")


def get_master_dir():
    return os.path.join(get_base_data_dir(), "master")


def get_reports_dir():
    return os.path.join(get_base_data_dir(), "reports")


def parse_raw_filename(filename: str):
    match = RAW_FILENAME_PATTERN.match(filename)
    if not match:
        return None

    info = match.groupdict()
    info["date"] = pd.to_datetime(info["date"]).normalize()
    return info


def normalize_strike_column(col_name):
    try:
        return float(str(col_name).strip())
    except (TypeError, ValueError):
        return None


def discover_raw_blocks(raw_dir: str, selected_tickers, start_date=None, end_date=None):
    blocks = {}

    for filename in sorted(os.listdir(raw_dir)):
        parsed = parse_raw_filename(filename)
        if parsed is None:
            continue

        ticker = parsed["ticker"]
        date = parsed["date"]

        if ticker not in selected_tickers:
            continue
        if start_date is not None and date < start_date:
            continue
        if end_date is not None and date > end_date:
            continue

        block_key = (ticker, date.strftime("%Y-%m-%d"), parsed["surface"], parsed["option_type"])
        if block_key not in blocks:
            blocks[block_key] = {
                "ticker": ticker,
                "date": date.strftime("%Y-%m-%d"),
                "surface": parsed["surface"],
                "option_type": parsed["option_type"],
                "files": {},
            }

        blocks[block_key]["files"][parsed["metric"]] = filename

    return list(blocks.values())


def get_master_dates_for_ticker(master_dir: str, ticker: str):
    path = os.path.join(master_dir, f"{ticker}_master.parquet")
    if not os.path.exists(path):
        return set()

    df = read_parquet_safe(path)
    if df.empty or "date" not in df.columns:
        return set()

    dates = pd.to_datetime(df["date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique().tolist()
    return set(dates)


def filter_blocks_to_sync_auto(blocks, master_dir):
    per_ticker_master_dates = {}
    filtered = []

    for block in blocks:
        ticker = block["ticker"]
        block_date = block["date"]

        if ticker not in per_ticker_master_dates:
            per_ticker_master_dates[ticker] = get_master_dates_for_ticker(master_dir, ticker)

        # V1 rule: sync only dates strictly newer than latest master date, or all if master missing
        existing_dates = per_ticker_master_dates[ticker]
        if not existing_dates:
            filtered.append(block)
            continue

        latest_master_date = max(existing_dates)
        if block_date > latest_master_date:
            filtered.append(block)

    return filtered


def load_surface_csv(path: str):
    df = pd.read_csv(path)
    if "Tenor" not in df.columns:
        raise ValueError("'Tenor' column missing")

    df = df.copy()
    df["Tenor"] = df["Tenor"].astype(str).str.strip()

    strike_map = {}
    for col in df.columns:
        if col == "Tenor":
            continue
        strike = normalize_strike_column(col)
        if strike is not None:
            strike_map[col] = strike

    if not strike_map:
        raise ValueError("No valid strike columns found")

    df = df[["Tenor"] + list(strike_map.keys())].rename(columns={"Tenor": "tenor", **strike_map})
    return df


def validate_block(raw_dir: str, block: dict):
    errors = []
    warnings = []
    stats = {
        "percent_rows": 0,
        "vol_rows": 0,
        "percent_non_null_cells": 0,
        "vol_non_null_cells": 0,
        "common_tenors_count": 0,
        "common_strikes_count": 0,
    }

    percent_file = block["files"].get("percent")
    vol_file = block["files"].get("vol")

    if not percent_file:
        errors.append("missing percent file")
    if not vol_file:
        errors.append("missing vol file")

    percent_df = None
    vol_df = None

    if percent_file:
        try:
            percent_df = load_surface_csv(os.path.join(raw_dir, percent_file))
            stats["percent_rows"] = len(percent_df)
            stats["percent_non_null_cells"] = int(percent_df.drop(columns=["tenor"]).notna().sum().sum())
        except Exception as exc:
            errors.append(f"percent file invalid: {exc}")

    if vol_file:
        try:
            vol_df = load_surface_csv(os.path.join(raw_dir, vol_file))
            stats["vol_rows"] = len(vol_df)
            stats["vol_non_null_cells"] = int(vol_df.drop(columns=["tenor"]).notna().sum().sum())
        except Exception as exc:
            errors.append(f"vol file invalid: {exc}")

    if percent_df is not None and vol_df is not None:
        percent_tenors = set(percent_df["tenor"].tolist())
        vol_tenors = set(vol_df["tenor"].tolist())
        common_tenors = percent_tenors & vol_tenors
        stats["common_tenors_count"] = len(common_tenors)

        percent_strikes = {c for c in percent_df.columns if c != "tenor"}
        vol_strikes = {c for c in vol_df.columns if c != "tenor"}
        common_strikes = percent_strikes & vol_strikes
        stats["common_strikes_count"] = len(common_strikes)

        if len(common_tenors) == 0:
            errors.append("no common tenors between percent and vol")
        if len(common_strikes) == 0:
            errors.append("no common strikes between percent and vol")

        if stats["percent_non_null_cells"] == 0:
            errors.append("percent file has zero non-null cells")
        if stats["vol_non_null_cells"] == 0:
            errors.append("vol file has zero non-null cells")

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "stats": stats,
    }


def surface_to_long(df_surface: pd.DataFrame, *, date_str: str, ticker: str, surface: str, option_type: str, metric: str):
    strike_cols = [col for col in df_surface.columns if col != "tenor"]

    df_long = df_surface.melt(
        id_vars="tenor",
        value_vars=strike_cols,
        var_name="strike_pct",
        value_name="metric_value",
    )

    df_long["date"] = pd.to_datetime(date_str)
    df_long["underlyer"] = ticker
    df_long["surface_type"] = surface
    df_long["option_type"] = option_type
    df_long["metric"] = metric
    df_long["strike_pct"] = pd.to_numeric(df_long["strike_pct"], errors="coerce")
    df_long["metric_value"] = pd.to_numeric(df_long["metric_value"], errors="coerce")

    return df_long.dropna(subset=["strike_pct"])


def transform_block_to_master_rows(raw_dir: str, block: dict):
    percent_file = block["files"]["percent"]
    vol_file = block["files"]["vol"]

    percent_df = load_surface_csv(os.path.join(raw_dir, percent_file))
    vol_df = load_surface_csv(os.path.join(raw_dir, vol_file))

    df_percent_long = surface_to_long(
        percent_df,
        date_str=block["date"],
        ticker=block["ticker"],
        surface=block["surface"],
        option_type=block["option_type"],
        metric="percent",
    )
    df_vol_long = surface_to_long(
        vol_df,
        date_str=block["date"],
        ticker=block["ticker"],
        surface=block["surface"],
        option_type=block["option_type"],
        metric="vol",
    )

    df_percent_long = df_percent_long.rename(
        columns={"metric_value": "price_percent"}
    ).drop(columns=["metric"])
    df_percent_long["source_price_file"] = percent_file

    df_vol_long = df_vol_long.rename(
        columns={"metric_value": "vol"}
    ).drop(columns=["metric"])
    df_vol_long["source_vol_file"] = vol_file

    merged = pd.merge(
        df_percent_long[KEY_COLS + ["price_percent", "source_price_file"]],
        df_vol_long[KEY_COLS + ["vol", "source_vol_file"]],
        on=KEY_COLS,
        how="outer",
    )

    merged["currency"] = merged["underlyer"].map(infer_currency)
    merged["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

    merged = merged[
        [
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
    ].copy()

    return merged


def infer_currency(underlyer: str) -> str:
    currencies = {
        "AS51": "AUD",
        "SX5E": "EUR",
        "DAX": "EUR",
        "HSCEI": "HKD",
        "HSI": "HKD",
        "KOSPI2": "KRW",
        "NKY": "JPY",
        "SMI": "CHF",
        "UKX": "GBP",
    }
    return currencies.get(underlyer, "USD")


def load_existing_master(master_dir: str, ticker: str):
    path = os.path.join(master_dir, f"{ticker}_master.parquet")
    if not os.path.exists(path):
        return pd.DataFrame(), path

    df = read_parquet_safe(path)
    if df.empty:
        return df, path

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["strike_pct"] = pd.to_numeric(df["strike_pct"], errors="coerce")
    df["price_percent"] = pd.to_numeric(df["price_percent"], errors="coerce")
    df["vol"] = pd.to_numeric(df["vol"], errors="coerce")
    return df, path


def merge_into_master(existing_df: pd.DataFrame, new_df: pd.DataFrame):
    if existing_df.empty:
        combined = new_df.copy()
        rows_replaced = 0
        rows_added = len(new_df)
    else:
        existing_keys = set(tuple(x) for x in existing_df[KEY_COLS].astype(str).to_numpy())
        new_keys = [tuple(x) for x in new_df[KEY_COLS].astype(str).to_numpy()]
        rows_replaced = sum(1 for key in new_keys if key in existing_keys)

        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=KEY_COLS, keep="last")
        rows_added = len(new_df) - rows_replaced

    combined = combined.sort_values(KEY_COLS).reset_index(drop=True)
    return combined, rows_added, rows_replaced


def aggregate_ticker_status(block_reports):
    ticker_map = {}

    for block in block_reports:
        ticker = block["ticker"]
        if ticker not in ticker_map:
            ticker_map[ticker] = {
                "ticker": ticker,
                "blocks_seen": 0,
                "blocks_valid": 0,
                "blocks_rejected": 0,
                "rows_added": 0,
                "rows_replaced": 0,
            }

        item = ticker_map[ticker]
        item["blocks_seen"] += 1
        if block["validation"]["is_valid"]:
            item["blocks_valid"] += 1
        else:
            item["blocks_rejected"] += 1

        item["rows_added"] += block["sync"]["rows_added"]
        item["rows_replaced"] += block["sync"]["rows_replaced"]

    ticker_rows = []
    for _, item in sorted(ticker_map.items()):
        if item["blocks_rejected"] == 0:
            item["status"] = "OK"
        elif item["blocks_valid"] > 0:
            item["status"] = "PARTIAL_SUCCESS"
        else:
            item["status"] = "FAILED"
        ticker_rows.append(item)

    return ticker_rows


def save_sync_report(report_payload, reports_dir, run_label):
    os.makedirs(reports_dir, exist_ok=True)
    json_path = os.path.join(reports_dir, f"sync_master_{run_label}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2)
    return json_path


def main():
    args = parse_args()

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [x.strip() for x in args.tickers.split(",") if x.strip()]

    start_date = pd.to_datetime(args.start_date).normalize() if args.start_date else None
    end_date = pd.to_datetime(args.end_date).normalize() if args.end_date else None

    raw_dir = get_raw_dir()
    master_dir = get_master_dir()
    reports_dir = get_reports_dir()
    os.makedirs(master_dir, exist_ok=True)

    run_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")

    blocks = discover_raw_blocks(
        raw_dir=raw_dir,
        selected_tickers=selected_tickers,
        start_date=start_date,
        end_date=end_date,
    )

    if start_date is None and end_date is None:
        blocks = filter_blocks_to_sync_auto(blocks, master_dir)

    print(f"Discovered {len(blocks)} raw blocks to evaluate", flush=True)

    block_reports = []
    valid_rows_per_ticker = {}

    if args.strict:
        strict_rejections = set()
        for ticker in selected_tickers:
            ticker_blocks = [b for b in blocks if b["ticker"] == ticker]
            if not ticker_blocks:
                continue

            validations = []
            for block in ticker_blocks:
                result = validate_block(raw_dir, block)
                validations.append((block, result))

            dates_with_invalid = {
                block["date"] for block, result in validations if not result["is_valid"]
            }

            for block, result in validations:
                if block["date"] in dates_with_invalid:
                    strict_rejections.add((block["ticker"], block["date"], block["surface"], block["option_type"]))

    for block in blocks:
        validation = validate_block(raw_dir, block)

        sync_status = {
            "status": "rejected",
            "rows_candidate": 0,
            "rows_added": 0,
            "rows_replaced": 0,
        }

        if args.strict and (block["ticker"], block["date"], block["surface"], block["option_type"]) in locals().get("strict_rejections", set()):
            if validation["is_valid"]:
                validation["warnings"].append("strict mode: rejected because another block for same ticker/date failed")
            validation["is_valid"] = False
            if "strict mode rejection" not in validation["errors"]:
                validation["errors"].append("strict mode rejection")

        if validation["is_valid"]:
            try:
                block_df = transform_block_to_master_rows(raw_dir, block)
                sync_status["status"] = "synced"
                sync_status["rows_candidate"] = len(block_df)

                if block["ticker"] not in valid_rows_per_ticker:
                    valid_rows_per_ticker[block["ticker"]] = []
                valid_rows_per_ticker[block["ticker"]].append(block_df)
            except Exception as exc:
                validation["is_valid"] = False
                validation["errors"].append(f"transform failed: {exc}")

        block_reports.append(
            {
                "ticker": block["ticker"],
                "date": block["date"],
                "surface": block["surface"],
                "option_type": block["option_type"],
                "files": {
                    "percent": block["files"].get("percent"),
                    "vol": block["files"].get("vol"),
                },
                "validation": validation,
                "sync": sync_status,
            }
        )

    total_rows_added = 0
    total_rows_replaced = 0
    tickers_updated = 0

    block_report_index = {
        (b["ticker"], b["date"], b["surface"], b["option_type"]): b
        for b in block_reports
    }

    for ticker, block_dfs in valid_rows_per_ticker.items():
        if not block_dfs:
            continue

        existing_df, master_path = load_existing_master(master_dir, ticker)
        new_df = pd.concat(block_dfs, ignore_index=True)
        new_df = new_df.drop_duplicates(subset=KEY_COLS, keep="last")

        combined_df, rows_added, rows_replaced = merge_into_master(existing_df, new_df)
        write_parquet_safe(combined_df, master_path)

        total_rows_added += rows_added
        total_rows_replaced += rows_replaced
        tickers_updated += 1

        # Allocate rows stats back to blocks proportionally/simple by block rows
        for block in [b for b in block_reports if b["ticker"] == ticker and b["validation"]["is_valid"]]:
            block_df = transform_block_to_master_rows(
                raw_dir,
                {
                    "ticker": block["ticker"],
                    "date": block["date"],
                    "surface": block["surface"],
                    "option_type": block["option_type"],
                    "files": block["files"],
                },
            )
            block["sync"]["rows_candidate"] = len(block_df)

        # For V1, set block rows_added/replaced conservatively:
        # rows_replaced per block = count of keys already present in existing_df
        if not existing_df.empty:
            existing_keys = set(tuple(x) for x in existing_df[KEY_COLS].astype(str).to_numpy())
        else:
            existing_keys = set()

        for block in [b for b in block_reports if b["ticker"] == ticker and b["validation"]["is_valid"]]:
            block_df = transform_block_to_master_rows(
                raw_dir,
                {
                    "ticker": block["ticker"],
                    "date": block["date"],
                    "surface": block["surface"],
                    "option_type": block["option_type"],
                    "files": block["files"],
                },
            )
            block_keys = [tuple(x) for x in block_df[KEY_COLS].astype(str).to_numpy()]
            block_rows_replaced = sum(1 for key in block_keys if key in existing_keys)
            block_rows_added = len(block_df) - block_rows_replaced

            block["sync"]["rows_added"] = block_rows_added
            block["sync"]["rows_replaced"] = block_rows_replaced

    blocks_seen = len(block_reports)
    blocks_valid = sum(1 for b in block_reports if b["validation"]["is_valid"])
    blocks_rejected = blocks_seen - blocks_valid

    if blocks_rejected == 0 and blocks_valid > 0:
        overall_status = "OK"
    elif blocks_valid > 0:
        overall_status = "PARTIAL_SUCCESS"
    else:
        overall_status = "FAILED"

    ticker_rows = aggregate_ticker_status(block_reports)

    report_payload = {
        "run": {
            "run_label": run_label,
            "started_at": started_at,
            "env": env,
            "raw_percent_dir": raw_dir,
            "master_dir": master_dir,
            "tickers_requested": selected_tickers,
            "start_date": start_date.strftime("%Y-%m-%d") if start_date is not None else None,
            "end_date": end_date.strftime("%Y-%m-%d") if end_date is not None else None,
            "strict_mode": args.strict,
        },
        "summary": {
            "blocks_seen": blocks_seen,
            "blocks_valid": blocks_valid,
            "blocks_rejected": blocks_rejected,
            "tickers_processed": len(selected_tickers),
            "tickers_updated": tickers_updated,
            "rows_candidate": int(sum(b["sync"]["rows_candidate"] for b in block_reports)),
            "rows_added": int(total_rows_added),
            "rows_replaced": int(total_rows_replaced),
            "status": overall_status,
        },
        "blocks": block_reports,
        "tickers": ticker_rows,
    }

    report_path = save_sync_report(report_payload, reports_dir, run_label)

    print(f"\nSync summary: {blocks_valid}/{blocks_seen} valid blocks, {blocks_rejected} rejected", flush=True)
    print(f"Tickers updated: {tickers_updated}", flush=True)
    print(f"Rows added: {total_rows_added}", flush=True)
    print(f"Rows replaced: {total_rows_replaced}", flush=True)
    print(f"Status: {overall_status}", flush=True)
    print(f"Report JSON: {report_path}", flush=True)


if __name__ == "__main__":
    main()
