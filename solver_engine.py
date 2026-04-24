import os
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from app import DATA_ROOT


LOOKBACK_DAYS = 365 * 2
MASTER_DIR = os.path.join(DATA_ROOT, "master")


def read_parquet_safe(path: str) -> pd.DataFrame:
    table = pq.read_table(path)
    return table.to_pandas()


def load_master_for_ticker(ticker: str) -> pd.DataFrame:
    path = os.path.join(MASTER_DIR, f"{ticker}_master.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Master file not found for {ticker}: {path}")

    df = read_parquet_safe(path)
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["strike_pct"] = pd.to_numeric(df["strike_pct"], errors="coerce")
    df["price_percent"] = pd.to_numeric(df["price_percent"], errors="coerce")

    df = df.dropna(
        subset=["date", "surface_type", "option_type", "tenor", "strike_pct", "price_percent"]
    ).reset_index(drop=True)

    return df.sort_values("date")


def build_node_cache(df_master: pd.DataFrame, surface_type: str):
    df = df_master[df_master["surface_type"] == surface_type].copy()
    cache = {}

    grouped = df.groupby(["option_type", "tenor", "strike_pct"], dropna=False)
    for key, group in grouped:
        s = (
            group[["date", "price_percent"]]
            .drop_duplicates("date")
            .set_index("date")["price_percent"]
            .sort_index()
        )
        cache[key] = s

    return cache


def strike_increment_from_tenor(tenor: str) -> float:
    if tenor in ["1w", "2w", "3w"]:
        return 0.5
    if tenor in ["1m", "2m", "3m"]:
        return 1.0
    return 2.0


def make_strike_grid(min_strike: float, max_strike: float, increment: float):
    min_strike = float(min_strike)
    max_strike = float(max_strike)

    if min_strike > max_strike:
        raise ValueError(f"Min strike {min_strike} cannot be greater than max strike {max_strike}")

    values = np.arange(min_strike, max_strike + increment / 2.0, increment)
    return np.round(values, 1).tolist()


def get_series(cache, option_type: str, tenor: str, strike: float) -> pd.Series:
    return cache.get((option_type, tenor, float(strike)), pd.Series(dtype=float))


def compute_combo_series(
    cache,
    type_1: str,
    tenor_1: str,
    strike_1: float,
    weight_1: float,
    type_2: str,
    tenor_2: str,
    strike_2: float,
    weight_2: float,
) -> pd.Series:
    s1 = get_series(cache, type_1, tenor_1, strike_1)
    s2 = get_series(cache, type_2, tenor_2, strike_2)

    if s1.empty or s2.empty:
        return pd.Series(dtype=float)

    df = pd.concat(
        [s1.rename("leg1"), s2.rename("leg2")],
        axis=1,
        sort=True,
    ).dropna()

    if df.empty:
        return pd.Series(dtype=float)

    combo = (df["leg1"] * float(weight_1)) + (df["leg2"] * float(weight_2))
    return combo.sort_index()


def rolling_percentile_prior(series: pd.Series, lookback_days: int = LOOKBACK_DAYS) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce").dropna().sort_index()
    if series.empty:
        return pd.Series(dtype=float)

    vals = series.to_numpy(dtype=float)
    dts = pd.to_datetime(series.index).to_numpy(dtype="datetime64[ns]")
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


def percentile_on_latest(series: pd.Series, lookback_days: int = LOOKBACK_DAYS):
    if series.empty:
        return np.nan, pd.NaT, np.nan, 0

    series = pd.to_numeric(series, errors="coerce").dropna().sort_index()
    if series.empty:
        return np.nan, pd.NaT, np.nan, 0

    latest_date = series.index.max()
    latest_value = float(series.loc[latest_date])

    cutoff = latest_date - pd.Timedelta(days=lookback_days)
    hist = series[(series.index >= cutoff) & (series.index < latest_date)]

    if hist.empty:
        return np.nan, latest_date, latest_value, 0

    percentile = 100.0 * (hist < latest_value).mean()
    return float(percentile), latest_date, latest_value, int(len(hist))


def build_combo_label(type_1, tenor_1, strike_1, weight_1, type_2, tenor_2, strike_2, weight_2):
    return (
        f"{weight_1:g} {type_1} {tenor_1} {float(strike_1):.1f} | "
        f"{weight_2:g} {type_2} {tenor_2} {float(strike_2):.1f}"
    )


def get_combo_detail(
    ticker: str,
    sf_value: str,
    type_1: str,
    tenor_1: str,
    strike_1: float,
    weight_1: float,
    type_2: str,
    tenor_2: str,
    strike_2: float,
    weight_2: float,
):
    if sf_value not in ["S", "F"]:
        raise ValueError("sf_value must be 'S' or 'F'")

    surface_type = "spot" if sf_value == "S" else "fwd"
    df_master = load_master_for_ticker(ticker)
    cache = build_node_cache(df_master, surface_type=surface_type)

    combo_series = compute_combo_series(
        cache=cache,
        type_1=type_1,
        tenor_1=tenor_1,
        strike_1=strike_1,
        weight_1=weight_1,
        type_2=type_2,
        tenor_2=tenor_2,
        strike_2=strike_2,
        weight_2=weight_2,
    )

    if combo_series.empty:
        return {
            "combo_label": build_combo_label(type_1, tenor_1, strike_1, weight_1, type_2, tenor_2, strike_2, weight_2),
            "latest_value": np.nan,
            "latest_percentile": np.nan,
            "latest_date": pd.NaT,
            "observation_count": 0,
            "price_series": pd.DataFrame(columns=["date", "value"]),
            "percentile_series": pd.DataFrame(columns=["date", "percentile"]),
            "surface_type": surface_type,
        }

    pct_series = rolling_percentile_prior(combo_series)
    latest_percentile, latest_date, latest_value, obs_count = percentile_on_latest(combo_series)

    price_df = pd.DataFrame(
        {
            "date": combo_series.index,
            "value": combo_series.values,
        }
    )

    percentile_df = pd.DataFrame(
        {
            "date": pct_series.index,
            "percentile": pct_series.values,
        }
    ).dropna()

    return {
        "combo_label": build_combo_label(type_1, tenor_1, strike_1, weight_1, type_2, tenor_2, strike_2, weight_2),
        "latest_value": latest_value,
        "latest_percentile": latest_percentile,
        "latest_date": latest_date,
        "observation_count": obs_count,
        "price_series": price_df,
        "percentile_series": percentile_df,
        "surface_type": surface_type,
    }


def build_solver_matrix(
    ticker: str,
    sf_value: str,
    type_1: str,
    tenor_1: str,
    min_strike_1: float,
    max_strike_1: float,
    weight_1: float,
    type_2: str,
    tenor_2: str,
    min_strike_2: float,
    max_strike_2: float,
    weight_2: float,
):
    if sf_value not in ["S", "F"]:
        raise ValueError("sf_value must be 'S' or 'F'")

    surface_type = "spot" if sf_value == "S" else "fwd"

    df_master = load_master_for_ticker(ticker)
    cache = build_node_cache(df_master, surface_type=surface_type)

    increment_1 = strike_increment_from_tenor(tenor_1)
    increment_2 = strike_increment_from_tenor(tenor_2)

    strike_group_1 = make_strike_grid(min_strike_1, max_strike_1, increment_1)
    strike_group_2 = make_strike_grid(min_strike_2, max_strike_2, increment_2)

    total_combos = len(strike_group_1) * len(strike_group_2)
    if total_combos > 400:
        raise ValueError(f"Grid too large: {total_combos} combinations. Please reduce strike ranges.")

    rows = []
    latest_dates = []

    for strike_1 in strike_group_1:
        for strike_2 in strike_group_2:
            combo_series = compute_combo_series(
                cache=cache,
                type_1=type_1,
                tenor_1=tenor_1,
                strike_1=strike_1,
                weight_1=weight_1,
                type_2=type_2,
                tenor_2=tenor_2,
                strike_2=strike_2,
                weight_2=weight_2,
            )

            percentile, latest_date, latest_value, obs_count = percentile_on_latest(combo_series)

            if pd.notna(latest_date):
                latest_dates.append(latest_date)

            rows.append(
                {
                    "Leg_1_Strike": strike_1,
                    "Leg_2_Strike": strike_2,
                    "Percentile": percentile,
                    "LatestValue": latest_value,
                    "Observations": obs_count,
                    "ComboLabel": build_combo_label(type_1, tenor_1, strike_1, weight_1, type_2, tenor_2, strike_2, weight_2),
                    "LatestDate": latest_date,
                }
            )

    result_df = pd.DataFrame(rows)
    pivot_df = result_df.pivot(index="Leg_2_Strike", columns="Leg_1_Strike", values="Percentile")
    value_df = result_df.pivot(index="Leg_2_Strike", columns="Leg_1_Strike", values="LatestValue")
    obs_df = result_df.pivot(index="Leg_2_Strike", columns="Leg_1_Strike", values="Observations")
    label_df = result_df.pivot(index="Leg_2_Strike", columns="Leg_1_Strike", values="ComboLabel")

    latest_date_used = max(latest_dates) if latest_dates else pd.NaT

    metadata = {
        "ticker": ticker,
        "surface_type": surface_type,
        "latest_date": latest_date_used,
        "total_combos": total_combos,
        "strike_group_1": strike_group_1,
        "strike_group_2": strike_group_2,
    }

    return pivot_df, value_df, obs_df, label_df, metadata
