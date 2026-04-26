import argparse
import requests
import xml.etree.ElementTree as ET
import os
import time
import math
from statistics import mean


TARGET_TICKERS = ["AS51", "DAX", "HSCEI", "HSI", "KOSPI2", "SMI", "UKX"]


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
                xml_data = response.text
                root = ET.fromstring(xml_data)
                token_element = root.find("Token")
                if token_element is None or not token_element.text:
                    raise RuntimeError("Authentication response did not contain a Token")
                return token_element.text
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == max_attempts:
                raise
            wait_seconds = backoff_seconds * attempt
            print(f"Auth attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {wait_seconds}s...", flush=True)
            time.sleep(wait_seconds)

    if last_error is not None:
        raise last_error


def extract_result(results, code):
    for item in results:
        if item.get("code") == code:
            value = item.get("value")
            if value is None:
                return None
            try:
                return float(str(value).replace(",", ""))
            except Exception:
                return None
    return None


def get_currency(ticker):
    if ticker == "SX5E":
        return "EUR"
    if ticker == "NKY":
        return "JPY"
    return "USD"


def get_block_definition(cp, block):
    if cp == "Call":
        short_strikes = [round(x * 0.5, 1) for x in range(200, 221)]
        mid_strikes = [round(x, 1) for x in range(100, 121)]
        long_strikes = [round(x * 2, 1) for x in range(50, 66)]
    else:
        short_strikes = [round(x * 0.5, 1) for x in range(180, 201)]
        mid_strikes = [round(x, 1) for x in range(80, 101)]
        long_strikes = [round(x * 2, 1) for x in range(35, 51)]

    if block == "short":
        return short_strikes, ["1w", "2w", "3w"]
    if block == "mid":
        return mid_strikes, ["1m", "2m", "3m"]
    if block == "long":
        return long_strikes, ["6m", "1y", "2y"]

    raise ValueError(f"Unknown block: {block}")


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
        instruments.append({
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
        })

    data = {
        "valuation": {"type": "EOD", "Date": date},
        "artifacts": {"underlyingAssets": {"EQ": ["MarketData"]}},
        "Instruments": instruments,
    }

    response = requests.post(url, headers=headers, json=data, timeout=60)
    response.raise_for_status()
    response_json = response.json()

    for instrument in response_json.get("instruments", []):
        instrument_id = instrument["id"]
        tenor_index = instrument_id - 1
        tenor = tenors[tenor_index]
        assets = instrument.get("assets", [])
        for asset in assets:
            results_list = asset.get("results", [])
            asset_strike = None
            forward_points = None
            initial_spot = None

            for result in results_list:
                code = result.get("code")
                value = result.get("value")
                if code == "ReferenceSpot":
                    asset_strike = value
                elif code == "AssetStrike":
                    asset_strike = value
                elif code == "InitialSpot":
                    initial_spot = value
                elif code == "ForwardPoints":
                    forward_points = value

            base_spot = asset_strike if asset_strike is not None else initial_spot
            if base_spot is not None and forward_points is not None:
                results[tenor] = float(str(base_spot).replace(",", "")) + float(str(forward_points).replace(",", ""))

    return results


def build_spot_payload(date, ticker, cp, ccy, strikes, tenors):
    instruments = []
    i = 0
    for strike in strikes:
        for tenor in tenors:
            i += 1
            instruments.append({
                "instrumentType": "Vanilla",
                "assetClass": "EQ",
                "ID": i,
                "buySell": "Buy",
                "callPut": cp,
                "payoutCurrency": ccy,
                "strike": f"{strike}%",
                "strikeDate": date,
                "expiryDate": tenor,
                "settlementDate": tenor,
                "style": "European",
                "underlyingAsset": {"bbgTicker": ticker},
                "volume": 1,
            })

    return {
        "valuation": {"type": "EOD", "Date": date},
        "artifacts": {"underlyingAssets": {}},
        "Instruments": instruments,
    }


def build_fwd_payload(token, date, ticker, cp, ccy, strikes, tenors):
    tenor_fwds = extract_price_and_forwardpoints(token, date, ticker, ccy)
    instruments = []
    i = 0

    for tenor in tenors:
        if tenor not in tenor_fwds:
            continue
        multiplier = tenor_fwds[tenor] / 100.0
        adjusted_strikes = [strike * multiplier for strike in strikes]
        for strike in adjusted_strikes:
            i += 1
            instruments.append({
                "instrumentType": "Vanilla",
                "assetClass": "EQ",
                "ID": i,
                "buySell": "Buy",
                "callPut": cp,
                "payoutCurrency": ccy,
                "strike": f"{strike}",
                "strikeDate": date,
                "expiryDate": tenor,
                "settlementDate": tenor,
                "style": "European",
                "underlyingAsset": {"bbgTicker": ticker},
                "volume": 1,
            })

    return {
        "valuation": {"type": "EOD", "Date": date},
        "artifacts": {"underlyingAssets": {}},
        "Instruments": instruments,
    }


def calculate(token, payload):
    url = "https://api.idd.pt.ice.com/eq/api/v1/Calculate"
    headers = {
        "AuthenticationToken": token,
        "Content-Type": "application/json",
    }
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def analyze_response(data, max_rows=8):
    rows = []
    for inst in data.get("instruments", []):
        results = inst.get("results", [])
        market_value_percent = extract_result(results, "MarketValuePercent")
        price_per_unit = extract_result(results, "PricePerUnit")
        market_value_mid = extract_result(results, "MarketValueMid")
        underlying_price = extract_result(results, "UnderlyingPrice")
        market_vol = extract_result(results, "MarketVol")

        calc_from_ppu = None
        calc_from_mid = None
        diff_ppu = None
        diff_mid = None

        if price_per_unit is not None and underlying_price not in (None, 0):
            calc_from_ppu = 100 * price_per_unit / underlying_price

        if market_value_mid is not None and underlying_price not in (None, 0):
            calc_from_mid = 100 * market_value_mid / underlying_price

        if market_value_percent is not None and calc_from_ppu is not None:
            diff_ppu = abs(market_value_percent - calc_from_ppu)

        if market_value_percent is not None and calc_from_mid is not None:
            diff_mid = abs(market_value_percent - calc_from_mid)

        rows.append({
            "id": inst.get("id"),
            "MarketValuePercent": market_value_percent,
            "PricePerUnit": price_per_unit,
            "MarketValueMid": market_value_mid,
            "UnderlyingPrice": underlying_price,
            "MarketVol": market_vol,
            "CalcFromPPU": calc_from_ppu,
            "CalcFromMid": calc_from_mid,
            "DiffPPU": diff_ppu,
            "DiffMid": diff_mid,
        })

    valid_ppu = [r["DiffPPU"] for r in rows if r["DiffPPU"] is not None]
    valid_mid = [r["DiffMid"] for r in rows if r["DiffMid"] is not None]

    print("Sample rows")
    for row in rows[:max_rows]:
        print(row)

    print("")
    print("Summary")
    print("  rows_total:", len(rows))
    print("  rows_with_market_value_percent:", sum(r["MarketValuePercent"] is not None for r in rows))
    print("  rows_with_price_per_unit:", sum(r["PricePerUnit"] is not None for r in rows))
    print("  rows_with_underlying_price:", sum(r["UnderlyingPrice"] is not None for r in rows))
    print("  avg_abs_diff_ppu:", mean(valid_ppu) if valid_ppu else None)
    print("  avg_abs_diff_mid:", mean(valid_mid) if valid_mid else None)

    return rows


def run_for_ticker(token, ticker, date, surface, cp, block):
    ccy = get_currency(ticker)
    strikes, tenors = get_block_definition(cp, block)

    if surface == "spot":
        payload = build_spot_payload(date, ticker, cp, ccy, strikes, tenors)
    else:
        payload = build_fwd_payload(token, date, ticker, cp, ccy, strikes, tenors)

    print("")
    print("=" * 100)
    print(f"TICKER={ticker} DATE={date} SURFACE={surface} CP={cp} BLOCK={block}")
    print("=" * 100)

    data = calculate(token, payload)
    return analyze_response(data)


def main():
    parser = argparse.ArgumentParser(description="Check MarketValuePercent reconstruction formula across tickers.")
    parser.add_argument("--date", default="2025-05-19")
    parser.add_argument("--surface", choices=["spot", "fwd"], default="spot")
    parser.add_argument("--cp", choices=["Call", "Put"], default="Call")
    parser.add_argument("--block", choices=["short", "mid", "long"], default="short")
    args = parser.parse_args()

    username = os.getenv("ICE_API_USERNAME")
    password = os.getenv("ICE_API_PASSWORD")
    if not username or not password:
        raise RuntimeError("Set ICE_API_USERNAME and ICE_API_PASSWORD first.")

    token = authenticate(username, password)

    print("REFERENCE TICKER: NDX")
    run_for_ticker(token, "NDX", args.date, args.surface, args.cp, args.block)

    for ticker in TARGET_TICKERS:
        run_for_ticker(token, ticker, args.date, args.surface, args.cp, args.block)


if __name__ == "__main__":
    main()
