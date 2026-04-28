import argparse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import time
import pandas as pd
import logging
import numpy as np
import json
from app import env, tickers
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


ICE_DEBUG = os.getenv("ICE_DEBUG", "").strip().lower() in {"1", "true", "yes", "y"}


def debug_print(*args, **kwargs):
    if ICE_DEBUG:
        print(*args, **kwargs)


def progress_iter(iterable, total, desc):
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)


def parse_float_maybe(value):
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def build_nan_block(strikes, tenors):
    percent_df = pd.DataFrame(
        np.nan,
        index=pd.Index(tenors, name="Tenor"),
        columns=pd.Index(strikes, name="Strike"),
        dtype=float,
    )
    vol_df = pd.DataFrame(
        np.nan,
        index=pd.Index(tenors, name="Tenor"),
        columns=pd.Index(strikes, name="Strike"),
        dtype=float,
    )
    return percent_df, vol_df


def get_base_data_dir():
    if env == "dev":
        return "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322"
    return "/mnt/disks/local-ssd/Options"


def get_raw_percent_dir():
    override = os.getenv("RAW_PERCENT_DIR")
    if override:
        return override
    return os.path.join(get_base_data_dir(), "raw_percent")


def get_reports_dir():
    return os.path.join(get_base_data_dir(), "reports")


def get_payout_currency(underlyer):
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


def authenticate(username, password, max_attempts=4, backoff_seconds=2):
    url = "https://api.idd.pt.ice.com/cm/Api/v1/Authenticate"
    headers = {"Content-Type": "application/xml"}
    data = f"""
    <Request>
        <Username>{username}</Username>
        <Password>{password}</Password>
    </Request>
    """

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(url, headers=headers, data=data, timeout=30)
            if response.status_code == 200:
                return response.text
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == max_attempts:
                raise
            wait_seconds = backoff_seconds * attempt
            print(
                f"Authentication attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {wait_seconds}s...",
                flush=True,
            )
            time.sleep(wait_seconds)

    if last_error is not None:
        raise last_error


def calculate(authentication_token, date, strikes, tenors, underlyer, CP, ccy="USD"):
    url = "https://api.idd.pt.ice.com/eq/api/v1/Calculate"
    headers = {
        "AuthenticationToken": authentication_token,
        "Content-Type": "application/json",
    }

    instruments = []
    i = 0
    for strike in strikes:
        for tenor in tenors:
            i += 1
            instruments.append(
                {
                    "instrumentType": "Vanilla",
                    "assetClass": "EQ",
                    "ID": i,
                    "buySell": "Buy",
                    "callPut": CP,
                    "payoutCurrency": ccy,
                    "strike": f"{strike}%",
                    "strikeDate": date,
                    "expiryDate": tenor,
                    "settlementDate": tenor,
                    "style": "European",
                    "underlyingAsset": {"bbgTicker": underlyer},
                    "volume": 1,
                }
            )

    data = {
        "valuation": {"type": "EOD", "Date": date},
        "artifacts": {"underlyingAssets": {}},
        "Instruments": instruments,
    }

    debug_print(
        "\nDEBUG ICE REQUEST",
        f"\n  ticker: {underlyer}",
        f"\n  date: {date}",
        f"\n  call_put: {CP}",
        f"\n  tenor_count: {len(tenors)}",
        f"\n  tenors: {','.join(tenors)}",
        f"\n  instruments: {len(instruments)}",
        f"\n  first_payload_item: {instruments[0] if instruments else None}",
        flush=True,
    )

    response = requests.post(url, headers=headers, json=data)

    debug_print(
        "\nDEBUG ICE RESPONSE",
        f"\n  status_code: {response.status_code}",
        f"\n  reason: {response.reason}",
        f"\n  body_preview: {response.text[:2000]}",
        flush=True,
    )

    if response.status_code == 200:
        return response.json()
    response.raise_for_status()


def parse_response_to_dataframe(data, strikes, tenors):
    df = pd.DataFrame(
        index=pd.Index(tenors, name="Tenor"),
        columns=pd.Index(strikes, name="Strike"),
        dtype=float,
    )
    df_vol = pd.DataFrame(
        index=pd.Index(tenors, name="Tenor"),
        columns=pd.Index(strikes, name="Strike"),
        dtype=float,
    )

    for instrument in data.get("instruments", []):
        instrument_id = instrument["id"]
        tenor_index = (instrument_id - 1) % len(tenors)
        strike_index = (instrument_id - 1) // len(tenors)

        if strike_index >= len(strikes) or tenor_index >= len(tenors):
            continue

        strike = strikes[strike_index]
        tenor = tenors[tenor_index]

        market_value_percent = next(
            (item["value"] for item in instrument.get("results", []) if item["code"] == "MarketValuePercent"),
            None,
        )
        market_vol = next(
            (item["value"] for item in instrument.get("results", []) if item["code"] == "MarketVol"),
            None,
        )

        market_value_percent = parse_float_maybe(market_value_percent)
        market_vol = parse_float_maybe(market_vol)

        if market_value_percent is not None:
            df.at[tenor, strike] = market_value_percent
        if market_vol is not None:
            df_vol.at[tenor, strike] = market_vol

    return df, df_vol


def generate_option_percent_df(auth_token, date, underlyer, CP, ccy="USD"):
    if CP == "Call":
        strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]
        strikes_2 = [round(x, 1) for x in range(100, 121)]
        strikes_3 = [round(x * 2, 1) for x in range(50, 66)]
    else:
        strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
        strikes_2 = [round(x, 1) for x in range(80, 101)]
        strikes_3 = [round(x * 2, 1) for x in range(35, 51)]

    tenors_1 = ["1w", "2w", "3w"]
    tenors_2 = ["1m", "2m", "3m"]
    tenors_3 = ["6m", "1y", "2y"]

    response_1 = calculate(auth_token, date, strikes_1, tenors_1, underlyer, CP, ccy)
    response_2 = calculate(auth_token, date, strikes_2, tenors_2, underlyer, CP, ccy)
    response_3 = calculate(auth_token, date, strikes_3, tenors_3, underlyer, CP, ccy)

    df_1, df_1_vol = parse_response_to_dataframe(response_1, strikes_1, tenors_1)
    df_2, df_2_vol = parse_response_to_dataframe(response_2, strikes_2, tenors_2)
    df_3, df_3_vol = parse_response_to_dataframe(response_3, strikes_3, tenors_3)

    df = pd.concat([df_1, df_2, df_3], axis=0)
    df_vol = pd.concat([df_1_vol, df_2_vol, df_3_vol], axis=0)

    return df.sort_index(axis=1), df_vol.sort_index(axis=1)


def extract_price_and_forwardpoints(authentication_token, date, underlyer, ccy="USD"):
    url = "https://api.idd.pt.ice.com/eq/api/v1/Calculate"
    headers = {
        "AuthenticationToken": authentication_token,
        "Content-Type": "application/json",
    }

    results = {}
    instruments = []
    tenors = ["1w", "2w", "3w", "1m", "2m", "3m", "6m", "9m", "1y", "2y"]

    for i, tenor in enumerate(tenors, start=1):
        instruments.append(
            {
                "instrumentType": "Vanilla",
                "assetClass": "EQ",
                "ID": i,
                "buySell": "Buy",
                "callPut": "Call",
                "payoutCurrency": ccy,
                "strike": "100%",
                "strikeDate": date,
                "expiryDate": tenor,
                "settlementDate": tenor,
                "style": "European",
                "underlyingAsset": {"bbgTicker": underlyer},
                "volume": 1,
            }
        )

    data = {
        "valuation": {"type": "EOD", "Date": date},
        "artifacts": {"underlyingAssets": {"EQ": ["MarketData"]}},
        "Instruments": instruments,
    }

    response = requests.post(url, headers=headers, json=data)

    debug_print(
        "\nFWD POINTS API",
        f"\n  ticker: {underlyer}",
        f"\n  date: {date}",
        f"\n  status_code: {response.status_code}",
        f"\n  reason: {response.reason}",
        f"\n  body_preview: {response.text[:2000]}",
        flush=True,
    )

    if response.status_code != 200:
        response.raise_for_status()

    response_json = response.json()

    for instrument in response_json.get("instruments", []):
        instrument_id = instrument["id"]
        tenor_index = instrument_id - 1
        if tenor_index < 0 or tenor_index >= len(tenors):
            continue

        tenor = tenors[tenor_index]
        assets = instrument.get("assets", [])

        for asset in assets:
            results_list = asset.get("results", [])

            base_spot = None
            forward_points = None

            for result in results_list:
                code = result.get("code")
                value = result.get("value")

                if code in {"ReferenceSpot", "AssetStrike", "InitialSpot"} and base_spot is None:
                    base_spot = parse_float_maybe(value)
                elif code == "ForwardPoints":
                    forward_points = parse_float_maybe(value)

            if base_spot is not None and forward_points is not None:
                results[tenor] = base_spot + forward_points

    debug_print(
        "\nFWD POINTS PARSED",
        f"\n  ticker: {underlyer}",
        f"\n  date: {date}",
        f"\n  parsed_tenors: {sorted(results.keys())}",
        f"\n  parsed_count: {len(results)}",
        flush=True,
    )

    return results


def calculate_fwd(authentication_token, date, tenor_strikes_dict, underlyer, CP, ccy="USD"):
    url = "https://api.idd.pt.ice.com/eq/api/v1/Calculate"
    headers = {
        "AuthenticationToken": authentication_token,
        "Content-Type": "application/json",
    }

    instruments = []
    i = 0
    for tenor, strikes in tenor_strikes_dict.items():
        for strike in strikes:
            i += 1
            instruments.append(
                {
                    "instrumentType": "Vanilla",
                    "assetClass": "EQ",
                    "ID": i,
                    "buySell": "Buy",
                    "callPut": CP,
                    "payoutCurrency": ccy,
                    "strike": f"{strike}",
                    "strikeDate": date,
                    "expiryDate": tenor,
                    "settlementDate": tenor,
                    "style": "European",
                    "underlyingAsset": {"bbgTicker": underlyer},
                    "volume": 1,
                }
            )

    data = {
        "valuation": {"type": "EOD", "Date": date},
        "artifacts": {"underlyingAssets": {}},
        "Instruments": instruments,
    }

    debug_print(
        "\nAPI CALL",
        f"\n  ticker: {underlyer}",
        f"\n  date: {date}",
        f"\n  call_put: {CP}",
        f"\n  tenor_count: {len(tenor_strikes_dict)}",
        f"\n  tenors: {','.join(tenor_strikes_dict.keys())}",
        f"\n  instruments: {len(instruments)}",
        f"\n  first_payload_item: {instruments[0] if instruments else None}",
        flush=True,
    )

    response = requests.post(url, headers=headers, json=data)

    debug_print(
        "\nAPI RESPONSE",
        f"\n  status_code: {response.status_code}",
        f"\n  reason: {response.reason}",
        f"\n  body_preview: {response.text[:2000]}",
        flush=True,
    )

    if response.status_code == 200:
        return response.json()
    response.raise_for_status()


def parse_response_to_dataframe_fwd(data, strikes, tenors):
    df = pd.DataFrame(
        index=pd.Index(tenors, name="Tenor"),
        columns=pd.Index(strikes, name="Strike"),
        dtype=float,
    )
    df_vol = pd.DataFrame(
        index=pd.Index(tenors, name="Tenor"),
        columns=pd.Index(strikes, name="Strike"),
        dtype=float,
    )

    for instrument in data.get("instruments", []):
        instrument_id = instrument["id"]
        tenor_index = (instrument_id - 1) // len(strikes)
        strike_index = (instrument_id - 1) % len(strikes)

        if strike_index >= len(strikes) or tenor_index >= len(tenors):
            continue

        strike = strikes[strike_index]
        tenor = tenors[tenor_index]

        market_value_percent = next(
            (item["value"] for item in instrument.get("results", []) if item["code"] == "MarketValuePercent"),
            None,
        )
        market_vol = next(
            (item["value"] for item in instrument.get("results", []) if item["code"] == "MarketVol"),
            None,
        )

        market_value_percent = parse_float_maybe(market_value_percent)
        market_vol = parse_float_maybe(market_vol)

        if market_value_percent is not None:
            df.at[tenor, strike] = market_value_percent
        if market_vol is not None:
            df_vol.at[tenor, strike] = market_vol

    debug_print(
        "\nPARSED BLOCK",
        f"\n  tenors: {','.join(tenors)}",
        f"\n  percent_shape: {df.shape}",
        f"\n  vol_shape: {df_vol.shape}",
        f"\n  percent_non_null: {int(df.notna().sum().sum())}",
        f"\n  vol_non_null: {int(df_vol.notna().sum().sum())}",
        flush=True,
    )

    return df, df_vol


def generate_fwd_option_percent_df(auth_token, date, underlyer, CP, tenor_strikes_dict, ccy="USD"):
    if CP == "Call":
        strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]
        strikes_2 = [round(x, 1) for x in range(100, 121)]
        strikes_3 = [round(x * 2, 1) for x in range(50, 66)]
    else:
        strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
        strikes_2 = [round(x, 1) for x in range(80, 101)]
        strikes_3 = [round(x * 2, 1) for x in range(35, 51)]

    tenors_1 = ["1w", "2w", "3w"]
    tenors_2 = ["1m", "2m", "3m"]
    tenors_3 = ["6m", "1y", "2y"]

    def apply_tenor_strike_mapping(strikes, tenors, tenor_spots):
        adjusted_strikes = {}
        for tenor in tenors:
            if tenor not in tenor_spots:
                raise KeyError(f"Tenor '{tenor}' not found in tenor_strikes_dict.")
            multiplier = tenor_spots[tenor] / 100.0
            adjusted_strikes[tenor] = [strike * multiplier for strike in strikes]
        return adjusted_strikes

    def process_block(block_strikes, block_tenors):
        missing_tenors = [tenor for tenor in block_tenors if tenor not in tenor_strikes_dict]
        if missing_tenors:
            print(
                f"  missing forward points -> NaN: {underlyer} {date} {CP} "
                f"tenors={','.join(block_tenors)} missing={missing_tenors}",
                flush=True,
            )
            return build_nan_block(block_strikes, block_tenors)

        try:
            adjusted_strikes = apply_tenor_strike_mapping(block_strikes, block_tenors, tenor_strikes_dict)
            response = calculate_fwd(auth_token, date, adjusted_strikes, underlyer, CP, ccy)
            df_block, df_block_vol = parse_response_to_dataframe_fwd(response, block_strikes, block_tenors)
            print(
                f"  ok block: {underlyer} {date} {CP} tenors={','.join(block_tenors)} missing=[]",
                flush=True,
            )
            return df_block, df_block_vol
        except Exception as exc:
            print(
                f"  failed block -> NaN: {underlyer} {date} {CP} "
                f"tenors={','.join(block_tenors)} error={exc}",
                flush=True,
            )
            logging.error(
                "Forward block failed for %s %s %s tenors=%s: %s",
                underlyer,
                date,
                CP,
                ",".join(block_tenors),
                exc,
            )
            return build_nan_block(block_strikes, block_tenors)

    df_1, df_1_vol = process_block(strikes_1, tenors_1)
    df_2, df_2_vol = process_block(strikes_2, tenors_2)
    df_3, df_3_vol = process_block(strikes_3, tenors_3)

    df = pd.concat([df_1, df_2, df_3], axis=0)
    df_vol = pd.concat([df_1_vol, df_2_vol, df_3_vol], axis=0)

    debug_print(
        "\nFINAL MERGE",
        f"\n  ticker: {underlyer}",
        f"\n  date: {date}",
        f"\n  call_put: {CP}",
        f"\n  percent_shape: {df.shape}",
        f"\n  vol_shape: {df_vol.shape}",
        f"\n  percent_non_null: {int(df.notna().sum().sum())}",
        f"\n  vol_non_null: {int(df_vol.notna().sum().sum())}",
        flush=True,
    )

    return df.sort_index(axis=1), df_vol.sort_index(axis=1)


def save_single_spot_option(auth_token, underlyer, date, CP, folder_name):
    try:
        ccy = get_payout_currency(underlyer)

        df, df_vol = generate_option_percent_df(auth_token, date, underlyer, CP, ccy)

        file_path = f"{folder_name}/{underlyer}_spot_{CP}_option_percent_{date}.csv"
        file_path_vol = f"{folder_name}/{underlyer}_spot_{CP}_option_vol_{date}.csv"

        df.to_csv(file_path)
        logging.info("Successfully saved: %s", file_path)

        df_vol.to_csv(file_path_vol)
        logging.info("Successfully saved: %s", file_path_vol)

    except Exception as e:
        logging.error("Error saving %s %s on %s: %s", underlyer, CP, date, e)
        raise


def save_spot_option_data(days_list, underlyers, auth_token, folder_name):
    tasks = []

    with ThreadPoolExecutor(max_workers=25) as executor:
        for underlyer in underlyers:
            for date in days_list:
                for CP in ["Call", "Put"]:
                    tasks.append(
                        executor.submit(
                            save_single_spot_option,
                            auth_token,
                            underlyer,
                            date,
                            CP,
                            folder_name,
                        )
                    )

        failures = []
        for future in progress_iter(as_completed(tasks), total=len(tasks), desc="Spot fetch tasks"):
            try:
                future.result()
            except Exception as e:
                failures.append(str(e))
                logging.error("Exception in thread: %s", e)

        if failures:
            raise RuntimeError(
                f"Spot option fetch failed for {len(failures)} task(s); first error: {failures[0]}"
            )


def save_single_fwd_option(auth_token, underlyer, date, CP, folder_name):
    try:
        ccy = get_payout_currency(underlyer)

        fwd_strikes = extract_price_and_forwardpoints(auth_token, date, underlyer, ccy)
        df, df_vol = generate_fwd_option_percent_df(auth_token, date, underlyer, CP, fwd_strikes, ccy)

        file_path = f"{folder_name}/{underlyer}_fwd_{CP}_option_percent_{date}.csv"
        file_path_vol = f"{folder_name}/{underlyer}_fwd_{CP}_option_vol_{date}.csv"

        df.to_csv(file_path)
        logging.info("Successfully saved: %s", file_path)

        df_vol.to_csv(file_path_vol)
        logging.info("Successfully saved: %s", file_path_vol)

    except Exception as e:
        logging.error("Error saving %s %s on %s: %s", underlyer, CP, date, e)
        raise


def save_fwd_option_data(days_list, underlyers, auth_token, folder_name):
    tasks = []

    with ThreadPoolExecutor(max_workers=25) as executor:
        for underlyer in underlyers:
            for date in days_list:
                for CP in ["Call", "Put"]:
                    tasks.append(
                        executor.submit(
                            save_single_fwd_option,
                            auth_token,
                            underlyer,
                            date,
                            CP,
                            folder_name,
                        )
                    )

        failures = []
        for future in progress_iter(as_completed(tasks), total=len(tasks), desc="Fwd fetch tasks"):
            try:
                future.result()
            except Exception as e:
                failures.append(str(e))
                logging.error("Exception in thread: %s", e)

        if failures:
            raise RuntimeError(
                f"Fwd option fetch failed for {len(failures)} task(s); first error: {failures[0]}"
            )


def expected_output_files(days_list, underlyers, include_spot=True, include_fwd=True):
    expected = []

    for underlyer in underlyers:
        for date in days_list:
            if include_spot:
                for cp in ["Call", "Put"]:
                    expected.append(
                        {
                            "ticker": underlyer,
                            "surface": "spot",
                            "option_type": cp,
                            "metric": "percent",
                            "date": date,
                            "filename": f"{underlyer}_spot_{cp}_option_percent_{date}.csv",
                        }
                    )
                    expected.append(
                        {
                            "ticker": underlyer,
                            "surface": "spot",
                            "option_type": cp,
                            "metric": "vol",
                            "date": date,
                            "filename": f"{underlyer}_spot_{cp}_option_vol_{date}.csv",
                        }
                    )

            if include_fwd:
                for cp in ["Call", "Put"]:
                    expected.append(
                        {
                            "ticker": underlyer,
                            "surface": "fwd",
                            "option_type": cp,
                            "metric": "percent",
                            "date": date,
                            "filename": f"{underlyer}_fwd_{cp}_option_percent_{date}.csv",
                        }
                    )
                    expected.append(
                        {
                            "ticker": underlyer,
                            "surface": "fwd",
                            "option_type": cp,
                            "metric": "vol",
                            "date": date,
                            "filename": f"{underlyer}_fwd_{cp}_option_vol_{date}.csv",
                        }
                    )

    return expected



def build_fetch_report(folder_name, expected_files):
    rows = []

    for item in expected_files:
        filename = item["filename"]
        full_path = os.path.join(folder_name, filename)
        exists = os.path.exists(full_path)
        size_bytes = os.path.getsize(full_path) if exists else 0

        row = dict(item)
        row["exists"] = exists
        row["size_bytes"] = size_bytes

        rows.append(row)

    return rows


def print_fetch_summary(report_items):
    total = len(report_items)
    ok = sum(1 for item in report_items if item["exists"])
    missing = total - ok

    print(f"\nFetch summary: {ok}/{total} files present, {missing} missing", flush=True)

    if missing > 0:
        print("Missing files:", flush=True)
        for item in [x for x in report_items if not x["exists"]][:100]:
            print(
                f"  ticker={item['ticker']} surface={item['surface']} "
                f"option_type={item['option_type']} metric={item['metric']} "
                f"date={item['date']} file={item['filename']}",
                flush=True,
            )


def save_fetch_report(report_items, reports_dir, run_label):
    os.makedirs(reports_dir, exist_ok=True)

    total = len(report_items)
    present = sum(1 for item in report_items if item["exists"])
    missing = total - present

    payload = {
        "summary": {
            "expected_files": total,
            "present_files": present,
            "missing_files": missing,
            "status": "OK" if missing == 0 else "INCOMPLETE",
            "run_label": run_label,
        },
        "files": report_items,
    }

    json_path = os.path.join(reports_dir, f"fetch_report_{run_label}.json")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return json_path



def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch spot and forward option percent/vol surfaces from ICE for a date range."
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start business date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End business date in YYYY-MM-DD format. Defaults to start date or today.",
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker list to fetch. Defaults to app.py tickers.",
    )
    parser.add_argument(
        "--spot-only",
        action="store_true",
        help="Fetch only spot files.",
    )
    parser.add_argument(
        "--fwd-only",
        action="store_true",
        help="Fetch only forward files.",
    )
    parser.add_argument(
        "--sync-master",
        action="store_true",
        help="Reserved for future master parquet sync once safeguards are implemented.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None

    if start_dt is None and end_dt is None:
        end_dt = datetime.today()
        start_dt = end_dt
    elif start_dt is None:
        start_dt = end_dt
    elif end_dt is None:
        end_dt = start_dt

    business_days = pd.bdate_range(start=start_dt, end=end_dt)
    business_days_list = business_days.strftime("%Y-%m-%d").tolist()

    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [item.strip() for item in args.tickers.split(",") if item.strip()]

    username = os.getenv("ICE_API_USERNAME")
    password = os.getenv("ICE_API_PASSWORD")
    if not username or not password:
        raise SystemExit("ICE_API_USERNAME and ICE_API_PASSWORD must be set in the environment.")

    base_data_dir = get_base_data_dir()
    raw_percent_dir = get_raw_percent_dir()
    reports_dir = get_reports_dir()

    os.makedirs(raw_percent_dir, exist_ok=True)

    if env == "dev":
        log_dir = os.path.join(base_data_dir, "logs", "Price", "Options")
    else:
        log_dir = os.path.join("/mnt/disks/local-ssd", "logs", "Price", "Options")

    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    log_filepath = os.path.join(log_dir, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO)

    today = datetime.today()

    logging.info(
        "Starting ICE fetch for %s business dates from %s to %s across %s tickers",
        len(business_days_list),
        business_days_list[0] if business_days_list else None,
        business_days_list[-1] if business_days_list else None,
        len(selected_tickers),
    )

    total_spot_tasks = 0 if args.fwd_only else len(business_days_list) * len(selected_tickers) * 2
    total_fwd_tasks = 0 if args.spot_only else len(business_days_list) * len(selected_tickers) * 2

    print(
        f"Fetch options: {len(selected_tickers)} tickers, {len(business_days_list)} business dates, "
        f"{total_spot_tasks} spot tasks, {total_fwd_tasks} fwd tasks",
        flush=True,
    )

    if args.sync_master:
        print(
            "sync-master requested, but not enabled yet until safeguards are implemented.",
            flush=True,
        )

    try:
        response = authenticate(username, password)
        logging.info("Successfully authenticated.")
    except requests.exceptions.RequestException as e:
        logging.error("Failed to authenticate: %s", e)
        raise SystemExit(f"Authentication failed: {e}")

    root = ET.fromstring(response)
    token_element = root.find("Token")
    if token_element is None or not token_element.text:
        logging.error("Authentication response did not contain a Token element")
        raise SystemExit("Authentication failed: response did not contain a Token")
    token = token_element.text

    try:
        if not args.fwd_only:
            save_spot_option_data(business_days_list, selected_tickers, token, raw_percent_dir)
        if not args.spot_only:
            save_fwd_option_data(business_days_list, selected_tickers, token, raw_percent_dir)

        logging.info("Successfully saved option data for %s", today)
    except Exception as e:
        logging.error("Failed to save option data for %s: %s", today, e)
        raise

    expected_files = expected_output_files(
        business_days_list,
        selected_tickers,
        include_spot=not args.fwd_only,
        include_fwd=not args.spot_only,
    )

    report_items = build_fetch_report(raw_percent_dir, expected_files)
    print_fetch_summary(report_items)

    run_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_report_path = save_fetch_report(report_items, reports_dir, run_label)

    missing_count = sum(1 for item in report_items if not item["exists"])
    present_count = sum(1 for item in report_items if item["exists"])

    print(f"\nReport JSON: {json_report_path}", flush=True)

    if missing_count == 0:
        print("\nFETCH STATUS: OK", flush=True)
        logging.info("FETCH STATUS: OK (%s/%s files present)", present_count, len(report_items))
    else:
        print(f"\nFETCH STATUS: INCOMPLETE ({missing_count} missing files)", flush=True)
        logging.warning(
            "FETCH STATUS: INCOMPLETE (%s missing out of %s expected files)",
            missing_count,
            len(report_items),
        )
