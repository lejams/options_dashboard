import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import pandas as pd
import logging
from io import StringIO
from app import env, tickers


def authenticate(username, password):
    url = 'https://api.idd.ice.com/cm/Api/v1/Authenticate'
    headers = {
        'Content-Type': 'application/xml'
    }
    data = f"""
    <Request>
        <Username>{username}</Username>
        <Password>{password}</Password>
    </Request>
    """
    
    response = requests.post(url, headers=headers, data=data)#, timeout=30)
    
    if response.status_code == 200:
        return response.text
    else:
        response.raise_for_status()


def calculate(authentication_token, date, strikes, tenors, underlyer, CP, ccy = 'USD'):
    """
    Sends a request to the ICE Data Derivatives (IDD) API to calculate the pricing for a list of vanilla options based 
    on the given strike prices, tenors, and underlying asset information.

    Parameters:
    ----------
    authentication_token : str
        The authentication token required to access the ICE API.
    date : str
        The valuation date (in "YYYY-MM-DD" format) for the options calculation.
    strikes : list of floats or ints
        A list of strike prices (in percentages) to calculate the options pricing.
    tenors : list of str
        A list of expiry dates (in "YYYY-MM-DD" format) representing the option tenors.
    underlyer : str
        The underlying asset's Bloomberg ticker (e.g., "AAPL US Equity").
    CP : str
        Specifies whether the option is a 'Call' or 'Put'.

    Returns:
    -------
    dict
        A JSON response from the ICE API containing the results of the options calculations.
    
    Raises:
    ------
    HTTPError
        If the request to the API fails (non-200 response), the function raises an HTTP error.

    Example:
    --------
    calculate(
        authentication_token="your_auth_token",
        date="2024-10-21",
        strikes=[100, 105, 110],
        tenors=["2025-01-01", "2025-07-01"],
        underlyer="AAPL US Equity",
        CP="Call"
    )
    """

    url = 'https://api.idd.ice.com/eq/api/v1/Calculate'
    headers = {
        'AuthenticationToken': authentication_token,
        'Content-Type': 'application/json'
    }
    
    # Create a list of instruments for each strike and tenor
    instruments = []
    i = 0
    for strike in strikes:
        for tenor in tenors:
            i +=1
            instruments.append({
                "instrumentType": "Vanilla",
                "assetClass": "EQ",
                "ID": 0 + i,#str(strike) + tenor,  # Unique ID for each instrument
                "buySell": "Buy",
                "callPut": CP,
                "payoutCurrency": ccy,
                "strike": f"{strike}%",
                "strikeDate": date,
                "expiryDate": tenor,
                "settlementDate": tenor,
                "style": "European",
                "underlyingAsset": {
                    "bbgTicker": underlyer
                },
                "volume": 1
            })

    # Data payload including all instruments
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
    
    response = requests.post(url, headers=headers, json=data)#, timeout=30)
    
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def parse_response_to_dataframe(data, strikes, tenors):
    # Initialize the DataFrame
    df = pd.DataFrame(index=pd.Index(tenors, name='Tenor'), columns=pd.Index(strikes, name='Strike'), dtype=float)
    
    # Since your instrument IDs increment sequentially by strike, then by tenor
    # We can reverse-engineer the ID to find the strike and tenor
    for instrument in data['instruments']:
        instrument_id = instrument['id']
        # Calculate the index based on ID, adjusting for your increment strategy
        tenor_index = (instrument_id -1) % len(tenors)
        strike_index = (instrument_id -1 ) // len(tenors)
        strike = strikes[strike_index]
        tenor = tenors[tenor_index]

        # Extract MarketValuePercent
        market_value_percent = next((item['value'] for item in instrument['results'] if item['code'] == 'MarketValuePercent'), None)
        
        # Place the value in the DataFrame
        if market_value_percent is not None:
            df.at[tenor, strike] = float(market_value_percent)
    return df


def generate_option_percent_df(auth_token, date, underlyer, CP, ccy = 'USD'):
    # Define strikes and tenors according to Call or Put
    if CP == "Call":
        strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]  # Adjusted range and rounding
        strikes_2 = [round(x, 1) for x in range(100, 121)]  # Simplified rounding
        strikes_3 = [round(x * 2, 1) for x in range(50, 66)]  # Corrected range end for Python's range
    else:
        strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
        strikes_2 = [round(x, 1) for x in range(80, 101)]  # Simplified rounding
        strikes_3 =  [round(x * 2, 1) for x in range(35, 51)] 

    tenors_1 = ['1w', '2w', '3w']
    tenors_2 = ['1m', '2m', '3m']
    tenors_3 = ['6m', '1y', '2y']

    # Make API calls for each strike and tenor group
    response_1 = calculate(auth_token, date, strikes_1, tenors_1, underlyer, CP, ccy)
    response_2 = calculate(auth_token, date, strikes_2, tenors_2, underlyer, CP, ccy)
    response_3 = calculate(auth_token, date, strikes_3, tenors_3, underlyer, CP, ccy)

    # Parse the responses into DataFrames
    df_1 = parse_response_to_dataframe(response_1, strikes_1, tenors_1)
    df_2 = parse_response_to_dataframe(response_2, strikes_2, tenors_2)
    df_3 = parse_response_to_dataframe(response_3, strikes_3, tenors_3)

    # Concatenate the DataFrames vertically (stacking them)
    df = pd.concat([df_1, df_2, df_3], axis=0)

    return df.sort_index(axis=1)

def save_spot_option_data(days_list, underlyers, auth_token, ccy = 'USD'):
    if env == 'dev':
        folder_name = r"/Users/omarbelhaj/Desktop/Financial Markets/New Indicators Dashboards/Data/Price/Options/raw_percent"
    else:
        folder_name = '/mnt/disks/local-ssd/Options/raw_percent'

    
    for underlyer in underlyers:
        if underlyer == 'SX5E':
            ccy = 'EUR'
        elif underlyer == 'NKY':
            ccy = 'JPY'
        else:
            ccy = 'USD'
        for date in days_list:
            for CP in ['Call', 'Put']:
                try:
                    # Generate the option percent prices DataFrame
                    df = generate_option_percent_df(auth_token, date, underlyer, CP, ccy)
                    if not os.path.exists(folder_name):
                        os.makedirs(folder_name)
                    file_path = f'{folder_name}/{underlyer}_spot_{CP}_option_percent_{date}.csv'
                    df.to_csv(file_path)
                    logging.info(f'Successfully saved: {file_path}')
            
                except Exception as e:
                    logging.info(f'Error saving {underlyer} {CP} on {date}: {e}')


############################################################################################################ FORWARD OPTION DATA ############################################################################################################

def extract_price_and_forwardpoints(authentication_token, date, underlyer, ccy = 'USD'):

    url = 'https://api.idd.ice.com/eq/api/v1/Calculate'
    headers = {
        'AuthenticationToken': authentication_token,
        'Content-Type': 'application/json'
    }
    
    results = {}
    # Create a list of instruments for each strike and tenor
    instruments = []
    i = 0
    tenors = ['1w', '2w', '3w', '1m', '2m', '3m', '6m', '9m','1y', '2y']
    for tenor in tenors:
        i += 1
        instruments.append({
            "instrumentType": "Vanilla",
            "assetClass": "EQ",
            "ID": i,  # Unique ID for each instrument
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
    
    # Data payload including all instruments
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

    
    response = requests.post(url, headers=headers, json=data)#, timeout=30)
    
    if response.status_code == 200:
        response_json = response.json()
        
        # Extract 'AssetStrike' and 'ForwardPoints' for each tenor
        for instrument in response_json.get('instruments', []):
            instrument_id = instrument['id']
            tenor_index = instrument_id - 1
            tenor = tenors[tenor_index]
            assets = instrument.get('assets', [])
            for asset in assets:
                results_list = asset.get('results', [])
                asset_strike = None
                forward_points = None
                for result in results_list:
                    if result['code'] == 'AssetStrike':
                        asset_strike = result['value']
                    elif result['code'] == 'ForwardPoints':
                        forward_points = result['value']

                results[tenor] = (float(asset_strike.replace(",", "")) + float(forward_points.replace(",", "")))

    
    else:
        response.raise_for_status()
    
    return results


def calculate_fwd(authentication_token, date, tenor_strikes_dict, underlyer, CP, ccy = 'USD'):
    """
    This function generates a forward option calculation for multiple tenors and strikes.
    
    Parameters:
    ----------
    authentication_token : str
        The authentication token required to access the ICE API.
    date : str
        The valuation date (in "YYYY-MM-DD" format).
    tenor_strikes_dict : dict
        A dictionary where the keys are tenors (e.g., '1w', '1m') and the values are lists of strikes.
    underlyer : str
        The underlying asset's Bloomberg ticker.
    CP : str
        Specifies whether the option is a 'Call' or 'Put'.
    
    Returns:
    -------
    dict
        A dictionary containing the API response data.
    """
    
    url = 'https://api.idd.ice.com/eq/api/v1/Calculate'
    headers = {
        'AuthenticationToken': authentication_token,
        'Content-Type': 'application/json'
    }
    
    # Create a list of instruments for each strike and tenor combination
    instruments = []
    i = 0
    for tenor, strikes in tenor_strikes_dict.items():  # Loop through each tenor and its list of strikes
        for strike in strikes:
            i += 1
            instruments.append({
                "instrumentType": "Vanilla",
                "assetClass": "EQ",
                "ID": i,  # Unique ID for each instrument
                "buySell": "Buy",
                "callPut": CP,
                "payoutCurrency": ccy,
                "strike": f"{strike}",  # Use the strike directly
                "strikeDate": date,
                "expiryDate": tenor,
                "settlementDate": tenor,
                "style": "European",
                "underlyingAsset": {
                    "bbgTicker": underlyer
                },
                "volume": 1
            })

    # Data payload including all instruments
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
    
    # Make the API request
    response = requests.post(url, headers=headers, json=data)# timeout=30)
    
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def parse_response_to_dataframe_fwd(data, strikes, tenors):
    # Initialize the DataFrame
    df = pd.DataFrame(index=pd.Index(tenors, name='Tenor'), columns=pd.Index(strikes, name='Strike'), dtype=float)
    
    # Since your instrument IDs increment sequentially by strike, then by tenor
    # We can reverse-engineer the ID to find the strike and tenor
    for instrument in data['instruments']:
        instrument_id = instrument['id']
        # Calculate the index based on ID, adjusting for your increment strategy
        tenor_index = (instrument_id -1) // len(strikes)
        strike_index = (instrument_id -1 ) % len(strikes)
        strike = strikes[strike_index]
        tenor = tenors[tenor_index]

        # Extract MarketValuePercent
        market_value_percent = next((item['value'] for item in instrument['results'] if item['code'] == 'MarketValuePercent'), None)
        
        # Place the value in the DataFrame
        if market_value_percent is not None:
            df.at[tenor, strike] = float(market_value_percent)
    return df


def generate_fwd_option_percent_df(auth_token, date, underlyer, CP, tenor_strikes_dict, ccy = 'USD'):
    """
    This function generates a DataFrame with forward option percentages by making API calls based on the provided
    tenor-to-strike mapping and option type (Call or Put).
    
    Parameters:
    ----------
    auth_token : str
        Authentication token for the API.
    date : str
        The valuation date (in "YYYY-MM-DD" format).
    underlyer : str
        The underlying asset's Bloomberg ticker.
    CP : str
        Specifies whether the option is a 'Call' or 'Put'.
    tenor_strikes_dict : dict
        A dictionary mapping each tenor (e.g., '1w', '1m') to the corresponding multiplier for the strike values.
    
    Returns:
    -------
    pd.DataFrame
        A DataFrame containing the calculated forward option percentages for each strike-tenor combination.
    """
    
    # Define strikes according to Call or Put
    if CP == "Call":
        strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]  # Adjusted range and rounding
        strikes_2 = [round(x, 1) for x in range(100, 121)]  # Simplified rounding
        strikes_3 = [round(x * 2, 1) for x in range(50, 66)]  # Corrected range end for Python's range
    else:
        strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
        strikes_2 = [round(x, 1) for x in range(80, 101)]  # Simplified rounding
        strikes_3 = [round(x * 2, 1) for x in range(35, 51)] 

    # Tenor groups
    tenors_1 = ['1w', '2w', '3w']
    tenors_2 = ['1m', '2m', '3m']
    tenors_3 = ['6m', '1y', '2y']

    # Apply the strike multiplier based on tenor_strikes_dict
    def apply_tenor_strike_mapping(strikes, tenors, tenor_strikes_dict):
        """
        Adjusts the strike values for each tenor using a multiplier from the tenor_strikes_dict.
        If a tenor is not found in the dictionary, raises a KeyError.
    
        Parameters:
        ----------
        strikes : list of float
            A list of strike values to be adjusted.
        tenors : list of str
            A list of tenors (e.g., '1w', '1m') that correspond to the strike values.
        tenor_strikes_dict : dict
            A dictionary mapping tenors to their corresponding strike multipliers.
    
        Returns:
        -------
        dict
            A dictionary where the keys are the tenors and the values are lists of adjusted strikes.
    
        Raises:
        -------
        KeyError
            If a tenor is not found in the tenor_strikes_dict.
        """
        adjusted_strikes = {}
    
        for tenor in tenors:
            if tenor not in tenor_strikes_dict:
                raise KeyError(f"Tenor '{tenor}' not found in tenor_strikes_dict.")
            
            multiplier = tenor_strikes_dict[tenor] / 100 # Get the multiplier, assuming it always exists now
            adjusted_strikes[tenor] = [strike * multiplier for strike in strikes]
    
        return adjusted_strikes


    # Adjust strikes for each tenor group based on the mapping
    adjusted_strikes_1 = apply_tenor_strike_mapping(strikes_1, tenors_1, tenor_strikes_dict)
    adjusted_strikes_2 = apply_tenor_strike_mapping(strikes_2, tenors_2, tenor_strikes_dict)
    adjusted_strikes_3 = apply_tenor_strike_mapping(strikes_3, tenors_3, tenor_strikes_dict)

    # Make API calls for each tenor group
    response_1 = calculate_fwd(auth_token, date, adjusted_strikes_1, underlyer, CP, ccy)
    response_2 = calculate_fwd(auth_token, date, adjusted_strikes_2, underlyer, CP, ccy)
    response_3 = calculate_fwd(auth_token, date, adjusted_strikes_3, underlyer, CP, ccy)

    # # Parse the responses into DataFrames
    df_1 = parse_response_to_dataframe_fwd(response_1, strikes_1, tenors_1)
    df_2 = parse_response_to_dataframe_fwd(response_2, strikes_2, tenors_2)
    df_3 = parse_response_to_dataframe_fwd(response_3, strikes_3, tenors_3)

    # Concatenate the DataFrames vertically (stacking them)
    df = pd.concat([df_1, df_2, df_3], axis=0)

    return df.sort_index(axis=1)


def save_fwd_option_data(days_list, underlyers, auth_token, ccy = 'USD'):
    if env == 'dev':
        folder_name = r"/Users/omarbelhaj/Desktop/Financial Markets/New Indicators Dashboards/Data/Price/Options/raw_percent"
    else:
        folder_name = '/mnt/disks/local-ssd/Options/raw_percent'
    
    for underlyer in underlyers:
        if underlyer == 'SX5E':
            ccy = 'EUR'
        elif underlyer == 'NKY':
            ccy = 'JPY'
        else:
            ccy = 'USD'
        for date in days_list:
            fwd_strikes = extract_price_and_forwardpoints(auth_token, date, underlyer, ccy)
            for CP in ['Call', 'Put']:
                try:
                    # Generate the option percent prices DataFrame
                    df = generate_fwd_option_percent_df(auth_token, date, underlyer, CP, fwd_strikes, ccy)
                    if not os.path.exists(folder_name):
                        os.makedirs(folder_name)
                    file_path = f'{folder_name}/{underlyer}_fwd_{CP}_option_percent_{date}.csv'
                    df.to_csv(file_path)
                    logging.info(f'Successfully saved: {file_path}')

            
                except Exception as e:
                    logging.info(f'Error saving {underlyer} {CP} on {date}: {e}')


def find_missing_files(date_list, tickers):
    folder_path = r"/Users/omarbelhaj/Desktop/Financial Markets/New Indicators Dashboards/Data/Price/Options/raw_percent"
    # Define the parameters for the filenames
    SF_options = ['spot', 'fwd']
    CP_options = ['Call', 'Put']
    
    # Initialize an empty list to store missing file names
    missing_files = []
    missing_dates = []

    # Loop through each combination of date, SF, CP, and ticker
    for ticker in tickers:
        for date in date_list:
            for SF in SF_options:
                for CP in CP_options:
                        # Construct the expected filename
                        expected_file = f"{ticker}_{SF}_{CP}_option_percent_{date}.csv"
                        # Check if the file exists in the folder
                        if not os.path.isfile(os.path.join(folder_path, expected_file)):
                            # If file is missing, add to the list
                            missing_files.append(expected_file)
                            missing_dates.append(date)
    
    # Print out the missing files
    if missing_files:
        print("Missing files:")
        for file in missing_files:
            print(file)
    else:
        print("All files are present.")
    return missing_dates


if __name__ == "__main__":
    today = datetime.today() - timedelta(days=0)
    start_date = today - timedelta(days= 0)
    business_days = pd.bdate_range(start=start_date, end=today)
    business_days_list = business_days.strftime("%Y-%m-%d").tolist()

    username = os.getenv("ICE_API_USERNAME")
    password = os.getenv("ICE_API_PASSWORD")
    if not username or not password:
        raise SystemExit("ICE_API_USERNAME and ICE_API_PASSWORD must be set in the environment.")

    if env == 'dev':
        log_dir = os.path.join('/Users/omarbelhaj/Desktop/Financial Markets/New Indicators Dashboards/logs', 'Price', 'Options')
    else:
        ssd_path = '/mnt/disks/local-ssd/'
        log_dir = os.path.join(ssd_path, 'logs', 'Price', 'Options')

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    log_filepath = os.path.join(log_dir, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO)

    today = datetime.today()

    try:
        response = authenticate(username, password)
        logging.info(f"Successfully authenticated: {response}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to authenticate: {e}")
    
    # Extract the Token value
    xml_data = response
    root = ET.fromstring(xml_data)
    token = root.find('Token').text

    try:
        save_spot_option_data(business_days_list, tickers, token)
        save_fwd_option_data(business_days_list, tickers, token)
        logging.info(f"Successfully saved option data for {today}")
    except Exception as e:
        logging.error(f"Failed to save option data for {today}: {e}")
