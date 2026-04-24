import argparse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import time
import pandas as pd
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from app import env

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def progress_iter(iterable, total, desc):
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)


def authenticate(username, password, max_attempts=4, backoff_seconds=2):
    url = 'https://api.idd.pt.ice.com/cm/Api/v1/Authenticate'
    headers = {
        'Content-Type': 'application/xml'
    }
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
                xml_data = response.text
                root = ET.fromstring(xml_data)
                token_element = root.find('Token')
                if token_element is None or not token_element.text:
                    raise RuntimeError("Authentication response did not contain a Token")
                return token_element.text
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


def extract_price_and_forwardpoints(authentication_token, date, underlyer, ccy='USD'):
    url = 'https://api.idd.pt.ice.com/eq/api/v1/Calculate'
    headers = {
        'AuthenticationToken': authentication_token,
        'Content-Type': 'application/json'
    }

    results = {}
    instruments = []
    i = 0
    tenors = ['1w', '2w', '3w', '1m', '2m', '3m', '6m', '9m', '1y', '2y']
    for tenor in tenors:
        i += 1
        instruments.append({
            "instrumentType": "Vanilla",
            "assetClass": "EQ",
            "ID": i,
            "buySell": "Buy",
            "callPut": 'Call',
            "payoutCurrency": ccy,
            "strike": "100%",
            "strikeDate": date,
            "expiryDate": tenor,
            "settlementDate": tenor,
            "style": "European",
            "underlyingAsset": {
                "bbgTicker": underlyer
            },
            "volume": 1
        })

    data = {
        "valuation": {
            "type": "EOD",
            "Date": date
        },
        "artifacts": {
            "underlyingAssets": {
                "EQ": [
                    "MarketData"
                ]
            }
        },
        "Instruments": instruments
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        response_json = response.json()

        for instrument in response_json.get('instruments', []):
            instrument_id = instrument['id']
            tenor_index = instrument_id - 1
            tenor = tenors[tenor_index]
            assets = instrument.get('assets', [])
            for asset in assets:
                results_list = asset.get('results', [])
                asset_strike = None
                forward_points = None
                initial_spot = None

                for result in results_list:
                    code = result.get('code')
                    value = result.get('value')

                    if code == 'ReferenceSpot':
                        asset_strike = value
                    elif code == 'AssetStrike':
                        asset_strike = value
                    elif code == 'InitialSpot':
                        initial_spot = value
                    elif code == 'ForwardPoints':
                        forward_points = value

                base_spot = asset_strike if asset_strike is not None else initial_spot

                if base_spot is not None and forward_points is not None:
                    results[tenor] = float(str(base_spot).replace(",", "")) + float(str(forward_points).replace(",", ""))
    else:
        response.raise_for_status()

    return results


def calculate_fwd(authentication_token, date, tenor_strikes_dict, underlyer, CP, ccy='USD'):
    url = 'https://api.idd.pt.ice.com/eq/api/v1/Calculate'
    headers = {
        'AuthenticationToken': authentication_token,
        'Content-Type': 'application/json'
    }

    instruments = []
    i = 0
    for tenor, strikes in tenor_strikes_dict.items():
        for strike in strikes:
            i += 1
            instruments.append({
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
                "underlyingAsset": {
                    "bbgTicker": underlyer
                },
                "volume": 1
            })

    data = {
        "valuation": {
            "type": "EOD",
            "Date": date
        },
        "artifacts": {
            "underlyingAssets": {}
        },
        "Instruments": instruments
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def parse_response_to_dataframe_fwd(data, strikes, tenors):
    df = pd.DataFrame(index=pd.Index(tenors, name='Tenor'), columns=pd.Index(strikes, name='Strike'), dtype=float)
    df_vol = pd.DataFrame(index=pd.Index(tenors, name='Tenor'), columns=pd.Index(strikes, name='Strike'), dtype=float)

    for instrument in data['instruments']:
        instrument_id = instrument['id']
        tenor_index = (instrument_id - 1) // len(strikes)
        strike_index = (instrument_id - 1) % len(strikes)
        strike = strikes[strike_index]
        tenor = tenors[tenor_index]

        market_value_percent = next((item['value'] for item in instrument['results'] if item['code'] == 'MarketValuePercent'), None)
        market_vol = next((item['value'] for item in instrument['results'] if item['code'] == 'MarketVol'), None)

        if market_value_percent is not None:
            df.at[tenor, strike] = float(market_value_percent)

        if market_vol is not None:
            df_vol.at[tenor, strike] = float(market_vol)

    return df, df_vol


def nan_block_fwd(strikes, tenors):
    df = pd.DataFrame(
        data=[[math.nan for _ in strikes] for _ in tenors],
        index=pd.Index(tenors, name='Tenor'),
        columns=pd.Index(strikes, name='Strike'),
        dtype=float,
    )
    df_vol = pd.DataFrame(
        data=[[math.nan for _ in strikes] for _ in tenors],
        index=pd.Index(tenors, name='Tenor'),
        columns=pd.Index(strikes, name='Strike'),
        dtype=float,
    )
    return df, df_vol


def generate_fwd_option_percent_df_partial(auth_token, date, underlyer, CP, tenor_strikes_dict, ccy='USD'):
    if CP == "Call":
        strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]
        strikes_2 = [round(x, 1) for x in range(100, 121)]
        strikes_3 = [round(x * 2, 1) for x in range(50, 66)]
    else:
        strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
        strikes_2 = [round(x, 1) for x in range(80, 101)]
        strikes_3 = [round(x * 2, 1) for x in range(35, 51)]

    tenors_1 = ['1w', '2w', '3w']
    tenors_2 = ['1m', '2m', '3m']
    tenors_3 = ['6m', '1y', '2y']

    def apply_tenor_strike_mapping(strikes, tenors, tenor_strikes_dict):
        adjusted_strikes = {}
        missing_tenors = []

        for tenor in tenors:
            if tenor not in tenor_strikes_dict:
                missing_tenors.append(tenor)
                continue

            multiplier = tenor_strikes_dict[tenor] / 100
            adjusted_strikes[tenor] = [strike * multiplier for strike in strikes]

        return adjusted_strikes, missing_tenors

    adjusted_strikes_1, missing_map_1 = apply_tenor_strike_mapping(strikes_1, tenors_1, tenor_strikes_dict)
    adjusted_strikes_2, missing_map_2 = apply_tenor_strike_mapping(strikes_2, tenors_2, tenor_strikes_dict)
    adjusted_strikes_3, missing_map_3 = apply_tenor_strike_mapping(strikes_3, tenors_3, tenor_strikes_dict)

    if not adjusted_strikes_1:
        logging.warning(f"{underlyer} {date} {CP} block {tenors_1} missing forward points: {missing_map_1}")
        df_1, df_1_vol = nan_block_fwd(strikes_1, tenors_1)
    else:
        try:
            response_1 = calculate_fwd(auth_token, date, adjusted_strikes_1, underlyer, CP, ccy)
            ok_tenors_1 = list(adjusted_strikes_1.keys())
            df_1, df_1_vol = parse_response_to_dataframe_fwd(response_1, strikes_1, ok_tenors_1)

            if missing_map_1:
                nan_df_1, nan_df_1_vol = nan_block_fwd(strikes_1, missing_map_1)
                df_1 = pd.concat([df_1, nan_df_1], axis=0)
                df_1_vol = pd.concat([df_1_vol, nan_df_1_vol], axis=0)

            df_1 = df_1.reindex(tenors_1)
            df_1_vol = df_1_vol.reindex(tenors_1)
        except Exception as e:
            logging.warning(f"{underlyer} {date} {CP} block {tenors_1} failed, filling NaN: {e}")
            df_1, df_1_vol = nan_block_fwd(strikes_1, tenors_1)

    if not adjusted_strikes_2:
        logging.warning(f"{underlyer} {date} {CP} block {tenors_2} missing forward points: {missing_map_2}")
        df_2, df_2_vol = nan_block_fwd(strikes_2, tenors_2)
    else:
        try:
            response_2 = calculate_fwd(auth_token, date, adjusted_strikes_2, underlyer, CP, ccy)
            ok_tenors_2 = list(adjusted_strikes_2.keys())
            df_2, df_2_vol = parse_response_to_dataframe_fwd(response_2, strikes_2, ok_tenors_2)

            if missing_map_2:
                nan_df_2, nan_df_2_vol = nan_block_fwd(strikes_2, missing_map_2)
                df_2 = pd.concat([df_2, nan_df_2], axis=0)
                df_2_vol = pd.concat([df_2_vol, nan_df_2_vol], axis=0)

            df_2 = df_2.reindex(tenors_2)
            df_2_vol = df_2_vol.reindex(tenors_2)
        except Exception as e:
            logging.warning(f"{underlyer} {date} {CP} block {tenors_2} failed, filling NaN: {e}")
            df_2, df_2_vol = nan_block_fwd(strikes_2, tenors_2)

    if not adjusted_strikes_3:
        logging.warning(f"{underlyer} {date} {CP} block {tenors_3} missing forward points: {missing_map_3}")
        df_3, df_3_vol = nan_block_fwd(strikes_3, tenors_3)
    else:
        try:
            response_3 = calculate_fwd(auth_token, date, adjusted_strikes_3, underlyer, CP, ccy)
            ok_tenors_3 = list(adjusted_strikes_3.keys())
            df_3, df_3_vol = parse_response_to_dataframe_fwd(response_3, strikes_3, ok_tenors_3)

            if missing_map_3:
                nan_df_3, nan_df_3_vol = nan_block_fwd(strikes_3, missing_map_3)
                df_3 = pd.concat([df_3, nan_df_3], axis=0)
                df_3_vol = pd.concat([df_3_vol, nan_df_3_vol], axis=0)

            df_3 = df_3.reindex(tenors_3)
            df_3_vol = df_3_vol.reindex(tenors_3)
        except Exception as e:
            logging.warning(f"{underlyer} {date} {CP} block {tenors_3} failed, filling NaN: {e}")
            df_3, df_3_vol = nan_block_fwd(strikes_3, tenors_3)

    df = pd.concat([df_1, df_2, df_3], axis=0)
    df_vol = pd.concat([df_1_vol, df_2_vol, df_3_vol], axis=0)

    return df.sort_index(axis=1), df_vol.sort_index(axis=1)


def save_single_fwd_option_partial(auth_token, underlyer, date, CP, folder_name):
    try:
        if underlyer == 'SX5E':
            ccy = 'EUR'
        elif underlyer == 'NKY':
            ccy = 'JPY'
        else:
            ccy = 'USD'

        fwd_strikes = extract_price_and_forwardpoints(auth_token, date, underlyer, ccy)
        df, df_vol = generate_fwd_option_percent_df_partial(auth_token, date, underlyer, CP, fwd_strikes, ccy)

        file_path = f'{folder_name}/{underlyer}_fwd_{CP}_option_percent_{date}.csv'
        file_path_vol = f'{folder_name}/{underlyer}_fwd_{CP}_option_vol_{date}.csv'

        df.to_csv(file_path)
        df_vol.to_csv(file_path_vol)

        logging.info(f'Successfully saved partial fwd percent: {file_path}')
        logging.info(f'Successfully saved partial fwd vol: {file_path_vol}')

    except Exception as e:
        logging.error(f'Error saving partial fwd {underlyer} {CP} on {date}: {e}')
        raise


def save_fwd_option_data_partial(days_list, underlyers, auth_token, call_puts):
    if env == 'dev':
        folder_name = '/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/raw_percent'
    else:
        folder_name = '/mnt/disks/local-ssd/Options/raw_percent'

    tasks = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        for underlyer in underlyers:
            for date in days_list:
                for CP in call_puts:
                    tasks.append(executor.submit(
                        save_single_fwd_option_partial,
                        auth_token,
                        underlyer,
                        date,
                        CP,
                        folder_name,
                    ))

        failures = []
        for future in progress_iter(as_completed(tasks), total=len(tasks), desc="Fwd partial fetch tasks"):
            try:
                future.result()
            except Exception as e:
                failures.append(repr(e))
                logging.exception("Exception in thread")

        if failures:
            print("")
            print(f"Completed with {len(failures)} partial failure(s). First error: {failures[0]}")
            print("Output files were still written where possible.")
            print("")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch forward option percent/vol surfaces from ICE with partial block tolerance."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start business date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End business date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-separated ticker list to fetch.",
    )

    parser.add_argument(
        "--call-put",
        default=None,
        choices=["Call", "Put"],
        help="Optional single option side to fetch. Defaults to both.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")

    business_days = pd.bdate_range(start=start_dt, end=end_dt)
    business_days_list = business_days.strftime("%Y-%m-%d").tolist()
    selected_tickers = [item.strip() for item in args.tickers.split(",") if item.strip()]
    selected_call_puts = [args.call_put] if args.call_put else ["Call", "Put"]


    username = os.getenv("ICE_API_USERNAME")
    password = os.getenv("ICE_API_PASSWORD")
    if not username or not password:
        raise SystemExit("ICE_API_USERNAME and ICE_API_PASSWORD must be set in the environment.")

    if env == 'dev':
        log_dir = os.path.join('/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/logs', 'Price', 'Options')
    else:
        ssd_path = '/mnt/disks/local-ssd/'
        log_dir = os.path.join(ssd_path, 'logs', 'Price', 'Options')

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = datetime.now().strftime('%Y-%m-%d') + '_fwd_partial.log'
    log_filepath = os.path.join(log_dir, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO)

    print(
        f"Fwd partial fetch: {len(selected_tickers)} ticker(s), {len(business_days_list)} business dates",
        flush=True,
    )

    try:
        token = authenticate(username, password)
        logging.info("Successfully authenticated for fwd partial fetch")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to authenticate: {e}")
        raise SystemExit(f"Authentication failed: {e}")
    except Exception as e:
        logging.error(f"Failed to parse authentication token: {e}")
        raise SystemExit(f"Authentication failed: {e}")

    try:
        save_fwd_option_data_partial(business_days_list, selected_tickers, token, selected_call_puts)
        logging.info("Successfully saved partial forward option data")
    except Exception as e:
        logging.error(f"Failed to save partial forward option data: {e}")
        raise
