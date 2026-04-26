from datetime import datetime, timedelta
import os
import numpy as np
import pandas as pd
import logging
from app import env, tickers, DATA_ROOT, LOG_ROOT, today_str
from io import StringIO


def load_files_and_calculate_percentiles(root_path, file_prefix, target_date_str, percentile_years):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    date_range_start = target_date - timedelta(days=percentile_years * 365)

    # Filter files based on prefix and date
    eligible_files = []

    directory_path = os.path.join(root_path, 'raw_percent')
    percentile_path = os.path.join(root_path, 'percentile')
    os.makedirs(percentile_path, exist_ok=True)

    files = os.listdir(directory_path)
    
    for filename in files:
        if filename.startswith(file_prefix) and filename.endswith('.csv'):
            file_date_str = filename.split('_')[-1].split('.')[0]
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
            if date_range_start <= file_date <= target_date:
                eligible_files.append(filename)

    # Collect data
    data = []
    for filename in eligible_files:
        df = pd.read_csv(os.path.join(directory_path, filename))

        for index, row in df.iterrows():
            tenor = row['Tenor']
            for strike in df.columns[1:]:
                value = row[strike]
                data.append((tenor, strike, value))

    # Convert to DataFrame
    collected_df = pd.DataFrame(data, columns=['Tenor', 'Strike', 'Value'])

    # Perform percentile calculations for the target date
    today_df = pd.read_csv(f'{directory_path}/{file_prefix}_{target_date_str}.csv')

    percentiles = []
    for index, row in today_df.iterrows():
        tenor = row['Tenor']
        for strike in today_df.columns[1:]:
            today_value= row[strike]
            historical_values = collected_df[(collected_df['Tenor'] == tenor) & (collected_df['Strike'] == strike)]['Value']
            historical_values.dropna(inplace= True)
            percentile = np.nansum(historical_values < today_value) / len(historical_values) * 100 if len(historical_values) > 0  else np.nan
            percentiles.append((tenor, strike, percentile))

    percentile_df = pd.DataFrame(percentiles, columns=['Tenor', 'Strike', 'Percentile'])
    pivot_df = percentile_df.pivot(index='Tenor', columns='Strike', values='Percentile')
    tenor_order = ['1w','2w', '3w', '1m', '2m', '3m', '6m', '1y', '2y']
    # Use pd.Categorical to set the custom order
    pivot_df.index = pd.Categorical (pivot_df.index, categories = tenor_order, ordered = True)
    pivot_df = pivot_df.sort_index()
    pivot_df.columns = pivot_df.columns.astype(float)
    pivot_df = pivot_df.sort_index(axis=1)

    # Save to CSV
    pivot_csv = os.path.join(percentile_path, f"{file_prefix}ile_{percentile_years}y_{target_date_str}.csv")
    pivot_df.to_csv(pivot_csv)

    return pivot_df


if __name__ == '__main__':
    if env == 'dev':
        log_dir = os.path.join(LOG_ROOT, 'Price', 'Options')
        directory_path = DATA_ROOT
    else:
        log_dir = '/mnt/disks/local-ssd/logs/Price/Options'
        directory_path = '/mnt/disks/local-ssd/Options'

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    log_filepath = os.path.join(log_dir, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO)

    target_date_str = today_str
    percentile_years = 2

    for CP in ['Call', 'Put']:
        for ticker in tickers:
            file_prefix = f'{ticker}_spot_{CP}_option_percent'
            file_prefix_fwd = f'{ticker}_fwd_{CP}_option_percent'

            try:
                load_files_and_calculate_percentiles(directory_path, file_prefix, target_date_str, percentile_years)
                logging.info(f'Successfully saved percentiles for {file_prefix} on {target_date_str}')
                load_files_and_calculate_percentiles(directory_path, file_prefix_fwd, target_date_str, percentile_years)
                logging.info(f'Successfully saved percentiles for {file_prefix_fwd} on {target_date_str}')
            except Exception as e: 
                logging.info(f'Error saving percentiles for {file_prefix} on {target_date_str}: {e}')
