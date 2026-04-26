import argparse
import json
import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app import DATA_ROOT, tickers


LOOKBACK_DAYS = 365 * 2


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build strategies master parquet files from option master parquet files."
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
        default=os.path.join(DATA_ROOT, "strategies_master"),
        help="Directory for <ticker>_strategies_master.parquet outputs.",
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


def load_master(master_path: str) -> pd.DataFrame:
    df = read_parquet_safe(master_path)

    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["strike_pct"] = pd.to_numeric(df["strike_pct"], errors="coerce")
    df["price_percent"] = pd.to_numeric(df["price_percent"], errors="coerce")

    df = df.dropna(
        subset=[
            "date",
            "surface_type",
            "option_type",
            "tenor",
            "strike_pct",
            "price_percent",
        ]
    )

    return df.sort_values("date").reset_index(drop=True)


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


def get_node_series(cache, surface_type, option_type, tenor, strike):
    return cache.get((surface_type, option_type, tenor, float(strike)), pd.Series(dtype=float))


def combo_to_label(combo):
    parts = []
    for tenor, strike, weight, option_type in combo:
        parts.append(f"{weight} {option_type} {tenor} {strike}")
    return " | ".join(parts)


def compute_two_leg_combo(cache, surface_type, combo):
    (tenor1, strike1, weight1, type1), (tenor2, strike2, weight2, type2) = combo

    s1 = get_node_series(cache, surface_type, type1, tenor1, strike1)
    s2 = get_node_series(cache, surface_type, type2, tenor2, strike2)

    if s1.empty or s2.empty:
        return pd.Series(dtype=float)

    df = pd.concat(
        [s1.rename("leg1"), s2.rename("leg2")],
        axis=1,
    ).dropna()

    if df.empty:
        return pd.Series(dtype=float)

    # Keep same economic convention as your old script
    return (df["leg1"] * weight1) - (df["leg2"] * weight2)


def compute_four_leg_combo(cache, surface_type, combo):
    weighted_legs = []

    for idx, (tenor, strike, weight, option_type) in enumerate(combo, start=1):
        s = get_node_series(cache, surface_type, option_type, tenor, strike)
        if s.empty:
            return pd.Series(dtype=float)
        weighted_legs.append((s.rename(f"leg{idx}") * weight))

    df = pd.concat(weighted_legs, axis=1).dropna()

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


def build_strategies_master_for_ticker(master_path: str, underlyer: str) -> pd.DataFrame:
    df_master = load_master(master_path)
    if df_master.empty:
        return pd.DataFrame()

    cache = build_node_cache(df_master)
    strategy_defs = get_strategy_definitions()

    outputs = []

    for family, (surface_type, combos) in strategy_defs.items():
        for combo in combos:
            if len(combo) == 2:
                combo_series = compute_two_leg_combo(cache, surface_type, combo)
            else:
                combo_series = compute_four_leg_combo(cache, surface_type, combo)

            rows = build_strategy_rows(
                underlyer=underlyer,
                strategy_family=family,
                surface_type=surface_type,
                combo=combo,
                combo_series=combo_series,
            )

            if not rows.empty:
                outputs.append(rows)

    if not outputs:
        return pd.DataFrame(
            columns=[
                "date",
                "underlyer",
                "surface_type",
                "strategy_family",
                "combination_label",
                "combination_json",
                "price",
                "percentile_2y",
            ]
        )

    return (
        pd.concat(outputs, ignore_index=True)
        .sort_values(["date", "strategy_family", "combination_label"])
        .reset_index(drop=True)
    )


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [x.strip() for x in args.tickers.split(",") if x.strip()]

    for ticker in selected_tickers:
        master_path = os.path.join(args.master_dir, f"{ticker}_master.parquet")
        output_path = os.path.join(args.output_dir, f"{ticker}_strategies_master.parquet")

        if not os.path.exists(master_path):
            print(f"[INFO] Missing master parquet for {ticker}: {master_path}", flush=True)
            continue

        if os.path.exists(output_path) and not args.overwrite:
            print(f"[INFO] Skipping {ticker}, output exists and --overwrite not set", flush=True)
            continue

        print(f"[RUN] Building strategies master for {ticker}", flush=True)
        df_out = build_strategies_master_for_ticker(master_path, ticker)
        write_parquet_safe(df_out, output_path)

        latest_date = df_out["date"].max() if not df_out.empty else None
        print(
            f"[OK] {ticker}: rows={len(df_out)}, latest_date={latest_date}, saved={output_path}",
            flush=True,
        )


if __name__ == "__main__":
    main()
