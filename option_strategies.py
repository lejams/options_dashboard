# import os
import argparse
# from datetime import datetime, timedelta
# import pandas as pd
# import numpy as np
# import logging
# from joblib import Parallel, delayed
# from app import env, tickers, DATA_ROOT, LOG_ROOT, today_str
# from io import StringIO


# def find_filenames(directory_path, underlyer, target_date_str, percentile_years, SF):
#     target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
#     one_year_ago = target_date - timedelta(days=percentile_years * 365)

#     def is_file_eligible(filename, option_type):
#         file_date_str = filename.split('_')[-1].split('.')[0]
#         try:
#             file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
#         except ValueError:
#             return False
#         conditions = [
#             filename.startswith(underlyer),
#             filename.endswith('.csv'),
#             'vol' not in filename,
#             option_type in filename,
#             ('spot' in filename) == (SF == 'S'),
#             one_year_ago <= file_date <= target_date
#         ]
#         return all(conditions)
    
#     if env == 'dev':
#         files = os.listdir(directory_path)
#     else:
#         ssd_path = '/mnt/disks/local-ssd/Options/raw_percent'
#         files = os.listdir(ssd_path)

#     eligible_files_Call = [f for f in files if is_file_eligible(f, 'Call')]
#     eligible_files_Put = [f for f in files if is_file_eligible(f, 'Put')]

#     return eligible_files_Call, eligible_files_Put


# def load_files_and_calculate_combo_price(directory_path, Call, Put, tenor1, strike1, weight1, tenor2, strike2, weight2, type1, type2):
#     def process_files(filenames, tenor, strike, option_type):
#         data = []
#         for filename in filenames:
#             if env == 'dev':
#                 df = pd.read_csv(os.path.join(directory_path, filename))
#             else:
#                 ssd_path = '/mnt/disks/local-ssd/Options/raw_percent'
#                 df = pd.read_csv(os.path.join(ssd_path, filename))
#             # df['Date'] = pd.to_datetime(df['Date'])

#             # if tenor in df.Tenor.values:
#             #     date = filename.split('_')[-1].split('.')[0]
#             #     value = df.loc[df['Tenor'] == tenor, str(strike)].values[0]
#             #     data.append({'Date': date, f'{option_type}_{tenor}_{strike}':value})

#             if tenor in df['Tenor'].values:
#                 if str(strike) in df.columns:
#                     date = filename.split('_')[-1].split('.')[0]
#                     value = df.loc[df['Tenor'] == tenor, str(strike)].values[0]
#                     data.append({'Date': date, f'{option_type}_{tenor}_{strike}': value})
#                 else:
#                     print(f"Strike {strike} missing in {filename} (columns: {df.columns.tolist()})")
#             else:
#                 print(f"Tenor {tenor} missing in {filename} (tenors: {df['Tenor'].values.tolist()})")

#         return pd.DataFrame(data)
    
#     df_Call = process_files(Call, tenor1, strike1, type1) if type1 == 'Call' else process_files(Put, tenor1, strike1, type1)
#     df_Put = process_files(Call, tenor2, strike2, type2) if type2 == 'Call' else process_files(Put, tenor2, strike2, type2)
#     if df_Call.equals(df_Put):
#         df_final = df_Call
#     else:
#         df_final = pd.merge(df_Call, df_Put, on='Date', how='outer')
#     df_final['Spread'] = (df_final[f'{type1}_{tenor1}_{strike1}'] * weight1) - (df_final[f'{type2}_{tenor2}_{strike2}'] * weight2)

#     # Fill missing values
#     df_final.fillna(0, inplace=True)

#     # Ensure that dates are in datetime format and sort
#     df_final['Date'] = pd.to_datetime(df_final['Date'])
#     df_final.sort_values(by='Date', inplace=True)

#     return df_final


# def calculate_single_combo_percentile(directory_path, eligible_Call, eligible_Put, combo, target_date):
#     """
#     Calculate the percentile for a single combo
#     """
#     (tenor1, strike1, weight1, type1 ),(tenor2, strike2, weight2, type2) = combo
#     df = load_files_and_calculate_combo_price(directory_path, eligible_Call, eligible_Put, tenor1, strike1, weight1, tenor2, strike2, weight2, type1, type2)
#     df['Date'] = pd.to_datetime(df['Date'])
#     today_row = df[df['Date'] == target_date]
#     if not today_row.empty:
#         today_value = today_row['Spread'].values[0]
#         historical_values = df[df['Date'] < target_date]['Spread']
#         percentile = np.sum(historical_values < today_value) / len(historical_values) * 100 if not historical_values.empty else np.nan
#         return(combo, today_value, percentile)
#     else:
#         logging.info(f'No data for {target_date} for combo {combo}')


# def calculate_combos_percentile(directory_path, underlyer, target_date_str, percentile_years, combinations, SF):
#     """
#     Calculate the percentiles for all combos
#     """
#     eligible_Call, eligible_Put = find_filenames(directory_path, underlyer, target_date_str, percentile_years, SF)
#     target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    
#     percentiles = Parallel(n_jobs=-1, backend = 'multiprocessing')(delayed(calculate_single_combo_percentile)(directory_path, eligible_Call, eligible_Put, combo, target_date) for combo in combinations)
#     temp_df = pd.DataFrame(percentiles, columns=['Combination', 'Price', 'Percentile'])
#     return temp_df


# def calculate_combos_percentile_4_trades(directory_path, underlyer, target_date_str, percentile_years, combinations, SF):
#     """
#     Calculate the percentile for a specific date and type for combinations.
#     """
#     eligible_Call, eligible_Put = find_filenames(directory_path, underlyer, target_date_str, percentile_years, SF)
#     percentiles = []
    
#     target_date = datetime.strptime(target_date_str, '%Y-%m-%d')

#     for combo in combinations:
#         (tenor1, strike1, weight1, type1 ),(tenor2, strike2, weight2, type2) = combo[:2]
#         (tenor3, strike3, weight3, type3 ),(tenor4, strike4, weight4, type4) = combo[2:]

#         df_pair1 = load_files_and_calculate_combo_price(directory_path, eligible_Call, eligible_Put, tenor1, strike1, weight1, tenor2, strike2, weight2, type1, type2)
#         df_pair2 = load_files_and_calculate_combo_price(directory_path, eligible_Call, eligible_Put, tenor3, strike3, weight3, tenor4, strike4, weight4, type3, type4)
#         df_merged = pd.merge(df_pair1, df_pair2, on='Date', suffixes=('_pair1', '_pair2'))  
#         df_merged['Combined Spread'] = (df_merged[f'{type1}_{tenor1}_{strike1}'] * weight1) + (df_merged[f'{type2}_{tenor2}_{strike2}'] * weight2) + (df_merged[f'{type3}_{tenor3}_{strike3}'] * weight3) + (df_merged[f'{type4}_{tenor4}_{strike4}'] * weight4) 
#         df_merged['Date'] = pd.to_datetime(df_merged['Date'])

#         today_row = df_merged[df_merged['Date'] == target_date]
#         if not today_row.empty:
#             today_result = today_row['Combined Spread'].iloc[0]
#             historical_results = df_merged[df_merged['Date'] < target_date]['Combined Spread']
#             percentile = np.sum(historical_results < today_result) / len(historical_results) * 100 if  len(historical_results)>0 else np.nan
#             percentiles.append((combo, today_result, percentile))
#         else:
#             print(f'No data for combo {combo} on target date {target_date_str}')
#             percentiles.append((combo, np.nan, np.nan))
        
#     temp_df = pd.DataFrame(percentiles, columns=['Combination', 'Price', 'Percentile'])

#     return temp_df

# def process_ticker(root_path, ticker, date_str, skew_combinations, straddle_combinations, strangles_combinations, call_spread_combinations, put_spread_combinations, call_ratio_combinations, put_ratio_combinations, call_calendar_combinations, put_calendar_combinations, iron_condor_combinations, iron_butterfly_combinations):
#     directory_path = os.path.join(root_path, 'raw_percent')
#     try:
#         #skews_f = calculate_combos_percentile(directory_path, ticker, date_str, 2, skew_combinations, 'F')
#         skews = calculate_combos_percentile(directory_path, ticker, date_str, 2, skew_combinations, 'F')
#         straddles = calculate_combos_percentile(directory_path, ticker, date_str, 2, straddle_combinations, 'F')
#         strangles = calculate_combos_percentile(directory_path, ticker, date_str, 2, strangles_combinations, 'F')
#         call_spreads = calculate_combos_percentile(directory_path, ticker, date_str, 2, call_spread_combinations, 'F')
#         put_spreads = calculate_combos_percentile(directory_path, ticker, date_str, 2, put_spread_combinations, 'F')
#         call_ratios = calculate_combos_percentile(directory_path, ticker, date_str, 2, call_ratio_combinations, 'F')
#         put_ratios = calculate_combos_percentile(directory_path, ticker, date_str, 2, put_ratio_combinations, 'F')
#         call_calendars = calculate_combos_percentile(directory_path, ticker, date_str, 2, call_calendar_combinations, 'S')
#         put_calendars = calculate_combos_percentile(directory_path, ticker, date_str, 2, put_calendar_combinations, 'S')
#         iron_condors = calculate_combos_percentile_4_trades(directory_path, ticker, date_str, 2, iron_condor_combinations, 'F')
#         iron_butterflies = calculate_combos_percentile_4_trades(directory_path, ticker, date_str, 2, iron_butterfly_combinations, 'F')

#         strategies_dir = os.path.join(root_path, 'strategies')

#         strategies_dataframes = [skews, straddles, strangles, call_spreads, put_spreads, call_ratios, put_ratios, call_calendars, put_calendars, iron_condors, iron_butterflies]
#         strategy_names = ['skews', 'straddles', 'strangles', 'call_spreads', 'put_spreads', 'call_ratios', 'put_ratios', 'call_calendars', 'put_calendars', 'iron_condors', 'iron_butterflies']

#         strategies = dict(zip(strategy_names, strategies_dataframes))

#         if env == 'dev':
#             if not os.path.exists(strategies_dir):
#                 os.makedirs(strategies_dir)
#         else:
#             ssd_path = '/mnt/disks/local-ssd/Options/strategies'
#             if not os.path.exists(ssd_path):
#                 os.makedirs(ssd_path)

#         for strategy_name, df in strategies.items():
#             file_path = os.path.join(strategies_dir, f'{ticker}_{strategy_name}_{date_str}.csv')
#             if env == 'dev':
#                 df.to_csv(file_path, index=False)
#                 logging.info(f'Successfully saved {file_path}')
#             else:
#                 ssd_path = '/mnt/disks/local-ssd/Options/strategies'
#                 local_ssd_path = os.path.join(ssd_path, f'{ticker}_{strategy_name}_{date_str}.csv')
#                 df.to_csv(local_ssd_path, index=False)
#                 logging.info(f'Successfully saved {local_ssd_path}')
#     except Exception as e:
#         logging.info(f'Error processing {ticker} on {date_str}: {e}')

# if __name__ == '__main__':
#     if env == 'dev':
#         # Define the log directory path
#         log_dir = os.path.join('/Users/omarbelhaj/Desktop/Financial Markets/New Indicators Dashboards/logs', 'Price', 'Options', 'Strategies')
#     else:
#         ssd_path = '/mnt/disks/local-ssd/'
#         log_dir = os.path.join(ssd_path, 'logs', 'Price', 'Options', 'Strategies')
#     # Create the log directory if it doesn't exist
#     if not os.path.exists(log_dir):
#         os.makedirs(log_dir)

#     # Define the log filename with the current date
#     log_filename = datetime.now().strftime('%Y-%m-%d.log')
#     log_filepath = os.path.join(log_dir, log_filename)

#     # Configure logging
#     logging.basicConfig(filename=log_filepath, level=logging.INFO,
#                         format='%(asctime)s:%(levelname)s:%(message)s')
    
#     #date_str = datetime.today().strftime('%Y-%m-%d')
#     date_str = (datetime.today() - timedelta(days=0)).strftime('%Y-%m-%d')

#     # 1) Skew Trades
#     skew_combinations = [  
#         (('1w', 99.0, 1, 'Put'), ('1w', 101.0, 1, 'Call')), (('2w', 98.5, 1, 'Put'), ('2w', 101.5, 1, 'Call')), (('3w', 97.5, 1, 'Put'), ('3w', 102.5, 1, 'Call')),
#         (('1m', 97.0, 1, 'Put'), ('1m', 103.0, 1, 'Call')), (('3m', 96.0, 1, 'Put'), ('3m', 104.0, 1, 'Call')),   
#         (('6m', 94.0, 1, 'Put'), ('6m', 106.0, 1, 'Call')), (('1y', 90.0, 1, 'Put'), ('1y', 110.0, 1, 'Call'))
#     ]

#     # 2) Straddle at the money
#     tenors = ['1w', '2w', '3w', '1m', '3m', '6m', '1y']
#     weight_group_1 = 1  
#     weight_group_2 = -1
#     straddle_combinations = []
#     strike = 100.0
#     for tenor in tenors:
#         straddle_combinations.append([(tenor, strike, weight_group_1, 'Call'), (tenor, strike, weight_group_2, 'Put')])

#      # 3) Strangles out of the money
#     strangles_combinations = [  
#         (('1w', 98.5, 1, 'Put'), ('1w', 101.5, -1, 'Call')),(('2w', 98.0, 1, 'Put'), ('2w', 102.0, -1, 'Call')),  (('3w', 97.5, 1, 'Put'), ('3w', 102.5, -1, 'Call')),
#         (('1m', 97.0, 1, 'Put'), ('1m', 103.0, -1, 'Call')), (('3m', 95.0, 1, 'Put'), ('3m', 105.0, -1, 'Call')),   
#         (('6m', 92.0, 1, 'Put'), ('6m', 108.0, -1, 'Call')), (('1y', 88.0, 1, 'Put'), ('1y', 112.0, -1, 'Call'))
#     ]

#      # 4) Call Spreads
#     call_spread_combinations = [  
#         (('1w', 101.0, 1, 'Call'), ('1w', 102.0, 1, 'Call')), (('2w', 101.5, 1, 'Call'), ('2w', 102.5, 1, 'Call')),  (('3w', 102.0, 1, 'Call'), ('3w', 103.0, 1, 'Call')),
#         (('1m', 103.0, 1, 'Call'), ('1m', 105.0, 1, 'Call')), (('3m', 104.0, 1, 'Call'), ('3m', 107.0, 1, 'Call')),   
#         (('6m', 106.0, 1, 'Call'), ('6m', 110.0, 1, 'Call')), (('1y', 108.0, 1, 'Call'), ('1y', 114.0, 1, 'Call'))
#     ]

#     # 5) Put Spreads
#     put_spread_combinations = [  
#         (('1w', 99.0, 1, 'Put'), ('1w', 98.0, 1, 'Put')), (('2w', 98.5, 1, 'Put'), ('2w', 97.5, 1, 'Put')),  (('3w', 98.0, 1, 'Put'), ('3w', 97.0, 1, 'Put')),
#         (('1m', 97.0, 1, 'Put'), ('1m', 95.0, 1, 'Put')), (('3m', 96.0, 1, 'Put'), ('3m', 93.0, 1, 'Put')),   
#         (('6m', 94.0, 1, 'Put'), ('6m', 90.0, 1, 'Put')), (('1y', 92.0, 1, 'Put'), ('1y', 86.0, 1, 'Put'))
#     ]

#      # 6) Call ratio
#     call_ratio_combinations = [  
#         (('1w', 101.0, 1, 'Call'), ('1w', 102.0, 2, 'Call')), (('2w', 101.5, 1, 'Call'), ('2w', 102.5, 2, 'Call')),  (('3w', 102.0, 1, 'Call'), ('3w', 103.0, 2, 'Call')),
#         (('1m', 103.0, 1, 'Call'), ('1m', 105.0, 2, 'Call')), (('3m', 104.0, 1, 'Call'), ('3m', 107.0, 2, 'Call')),   
#         (('6m', 106.0, 1, 'Call'), ('6m', 110.0, 2, 'Call')), (('1y', 108.0, 1, 'Call'), ('1y', 114.0, 2, 'Call'))
#     ]

#     # 7) Put Ratio
#     put_ratio_combinations = [  
#         (('1w', 99.0, 1, 'Put'), ('1w', 98.0, 2, 'Put')), (('2w', 98.5, 1, 'Put'), ('2w', 97.5, 2, 'Put')),  (('3w', 98.0, 1, 'Put'), ('3w', 97.0, 2, 'Put')),
#         (('1m', 97.0, 1, 'Put'), ('1m', 95.0, 2, 'Put')), (('3m', 96.0, 1, 'Put'), ('3m', 93.0, 2, 'Put')),   
#         (('6m', 94.0, 1, 'Put'), ('6m', 90.0, 2, 'Put')), (('1y', 92.0, 1, 'Put'), ('1y', 86.0, 2, 'Put'))
#     ]

#     # 8) Call Calendar
#     call_calendar_combinations = [
#         (('1w', 101.0, -1, 'Call'), ('1m', 101.0, -1, 'Call')), (('1w', 101.0, -1, 'Call'), ('2m', 101.0, -1, 'Call')),  (('1w', 101.0, -1, 'Call'), ('3m', 101.0, -1, 'Call')),
#         (('2w', 102.0, -1, 'Call'), ('1m', 102.0, -1, 'Call')), (('2w', 102.0, -1, 'Call'), ('2m', 102.0, -1, 'Call')), (('2w', 102.0, -1, 'Call'), ('3m', 102.0, -1, 'Call')),   
#         (('1m', 103.0, -1, 'Call'), ('3m', 103.0, -1, 'Call')), (('1m', 104.0, -1, 'Call'), ('6m', 104.0, -1, 'Call')), #[('1m', 104.0, -1, 'Call'), ('9m', 107.0, -1, 'Call')],   
#         (('3m', 106.0, -1, 'Call'), ('6m', 106.0, -1, 'Call')), (('3m', 106.0, -1, 'Call'), ('1y', 106.0, -1, 'Call'))
#     ]

#     # 9) Put Calendar
#     put_calendar_combinations = [
#         (('1w', 99.0, -1, 'Put'), ('1m', 99.0, -1, 'Put')), (('1w', 99.0, -1, 'Put'), ('2m', 99.0, -1, 'Put')),  (('1w', 99.0, -1, 'Put'), ('3m', 99.0, -1, 'Put')),
#         (('2w', 98.0, -1, 'Put'), ('1m', 98.0, -1, 'Put')), (('2w', 98.0, -1, 'Put'), ('2m', 98.0, -1, 'Put')), (('2w', 98.0, -1, 'Put'), ('3m', 98.0, -1, 'Put')),   
#         (('1m', 97.0, -1, 'Put'), ('3m', 97.0, -1, 'Put')), (('1m', 96.0, -1, 'Put'), ('6m', 96.0, -1, 'Put')), #[('1m', 104.0, -1, 'Put'), ('9m', 107.0, -1, 'Put')],   
#         (('3m', 94.0, -1, 'Put'), ('6m', 94.0, -1, 'Put')), (('3m', 94.0, -1, 'Put'), ('1y', 94.0, -1, 'Put'))
#     ]

#     # 10) Iron Condor
#     iron_condors_combinations = [
#         (('1w', 99.0, 1, 'Put'), ('1w', 98.5, -1, 'Put'), ('1w', 101.0, 1, 'Call'), ('1w', 101.5, -1, 'Call')),  
#         (('2w', 98.5, 1, 'Put'), ('2w', 98.0, -1, 'Put'), ('2w', 101.5, 1, 'Call'), ('2w', 102.0, -1, 'Call')),  
#         (('3w', 98.0, 1, 'Put'), ('3w', 97.0, -1, 'Put'), ('3w', 102.0, 1, 'Call'), ('3w', 103.0, -1, 'Call')), 
#         (('1m', 97.0, 1, 'Put'), ('1m', 95.0, -1, 'Put'), ('1m', 103.0, 1, 'Call'), ('1m', 105.0, -1, 'Call')),
#         (('3m', 96.0, 1, 'Put'), ('3m', 94.0, -1, 'Put'), ('3m', 104.0, 1, 'Call'), ('3m', 106.0, -1, 'Call')),
#         (('6m', 92.0, 1, 'Put'), ('6m', 90.0, -1, 'Put'), ('6m', 108.0, 1, 'Call'), ('6m', 110.0, -1, 'Call')),
#     ]

#     # 11) Iron Butterfly
#     iron_butterfly_combinations = [
#         (('1w', 100.0, 1, 'Put'), ('1w', 100.0, 1, 'Call'), ('1w', 98.5, -1, 'Put'), ('1w', 101.5, -1, 'Call')),  
#         (('2w', 100.0, 1, 'Put'), ('2w', 100.0, 1, 'Call'), ('2w', 98.0, -1, 'Put'), ('2w', 102.0, -1, 'Call')),  
#         (('3w', 100.0, 1, 'Put'), ('3w', 100.0, 1, 'Call'), ('3w', 97.5, -1, 'Put'), ('3w', 102.5, -1, 'Call')), 
#         (('1m', 100.0, 1, 'Put'), ('1m', 100.0, 1, 'Call'), ('1m', 97.0, -1, 'Put'), ('1m', 103.0, -1, 'Call')),
#         (('3m', 100.0, 1, 'Put'), ('3m', 100.0, 1, 'Call'), ('3m', 95.0, -1, 'Put'), ('3m', 105.0, -1, 'Call')),
#         (('6m', 100.0, 1, 'Put'), ('6m', 100.0, 1, 'Call'), ('6m', 92.0, -1, 'Put'), ('6m', 108.0, -1, 'Call')),
#     ]

#     path = r"/Users/omarbelhaj/Desktop/Financial Markets/New Indicators Dashboards/Data/Price/Options"


#     for ticker in tickers:
#         process_ticker(path, ticker, date_str, skew_combinations, straddle_combinations, strangles_combinations, call_spread_combinations, put_spread_combinations, call_ratio_combinations, put_ratio_combinations, call_calendar_combinations, put_calendar_combinations, iron_condors_combinations, iron_butterfly_combinations)

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from app import env, tickers, DATA_ROOT, LOG_ROOT, today_str
from tqdm import tqdm


def find_filenames(directory_path, underlyer, target_date_str, percentile_years, SF):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    one_year_ago = target_date - timedelta(days=percentile_years * 365)

    def is_file_eligible(filename, option_type):
        try:
            file_date = datetime.strptime(filename.split('_')[-1].split('.')[0], '%Y-%m-%d')
        except Exception:
            return False
        conditions = [
            filename.startswith(underlyer),
            filename.endswith('.csv'),
            'vol' not in filename,
            option_type in filename,
            ('spot' in filename) == (SF == 'S'),
            one_year_ago <= file_date <= target_date
        ]
        return all(conditions)
    
    if env == 'dev':
        files = os.listdir(directory_path)
    else:
        ssd_path = '/mnt/disks/local-ssd/Options/raw_percent'
        files = os.listdir(ssd_path)
    eligible_files_Call = [f for f in files if is_file_eligible(f, 'Call')]
    eligible_files_Put = [f for f in files if is_file_eligible(f, 'Put')]

    return eligible_files_Call, eligible_files_Put

def read_and_extract(path, tenor, strike, option_type):
    try:
        df = pd.read_csv(path)
        if tenor in df['Tenor'].values:
            date = os.path.basename(path).split('_')[-1].split('.')[0]
            value = df.loc[df['Tenor'] == tenor, str(strike)].values[0]
            return {'Date': date, f'{option_type}_{tenor}_{strike}': value}
    except Exception as e:
        logging.error(f"Error processing {path}: {e}")
    return None

def process_files(directory_path, filenames, tenor, strike, option_type):
    paths = [os.path.join(directory_path, f) for f in filenames]
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(lambda path: read_and_extract(path, tenor, strike, option_type), paths))
    
    results = [r for r in results if r is not None]
    return pd.DataFrame(results)


def load_files_and_calculate_combo_price(directory_path, Call, Put, tenor1, strike1, weight1, tenor2, strike2, weight2, type1, type2):
    df_Call = process_files(directory_path, Call, tenor1, strike1, type1) if type1 == 'Call' else process_files(directory_path, Put, tenor1, strike1, type1)
    df_Put = process_files(directory_path, Call, tenor2, strike2, type2) if type2 == 'Call' else process_files(directory_path, Put, tenor2, strike2, type2)

    if df_Call.equals(df_Put):
        df_final = df_Call
    else:
        df_final = pd.merge(df_Call, df_Put, on='Date', how='outer')

    df_final['Spread'] = (df_final[f'{type1}_{tenor1}_{strike1}'] * weight1) - (df_final[f'{type2}_{tenor2}_{strike2}'] * weight2)
    df_final.fillna(0, inplace=True)
    df_final['Date'] = pd.to_datetime(df_final['Date'], errors='coerce')
    df_final.dropna(subset=['Date'], inplace=True)
    df_final.sort_values(by='Date', inplace=True)
    return df_final

def calculate_single_combo_percentile(directory_path, eligible_Call, eligible_Put, combo, target_date):
    (tenor1, strike1, weight1, type1), (tenor2, strike2, weight2, type2) = combo
    df = load_files_and_calculate_combo_price(directory_path, eligible_Call, eligible_Put,
                                               tenor1, strike1, weight1, tenor2, strike2, weight2, type1, type2)
    today_row = df[df['Date'] == target_date]
    if not today_row.empty:
        today_value = today_row['Spread'].iloc[0]
        historical = df[df['Date'] < target_date]['Spread']
        percentile = np.sum(historical < today_value) / len(historical) * 100 if not historical.empty else np.nan
        return (combo, today_value, percentile)
    else:
        logging.info(f"No data for {target_date} for combo {combo}")
        return (combo, np.nan, np.nan)

def calculate_combos_percentile(directory_path, underlyer, target_date_str, percentile_years, combinations, SF):
    eligible_Call, eligible_Put = find_filenames(directory_path, underlyer, target_date_str, percentile_years, SF)
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [
            executor.submit(calculate_single_combo_percentile, directory_path, eligible_Call, eligible_Put, combo, target_date)
            for combo in combinations
        ]
        results = [f.result() for f in futures]

    df = pd.DataFrame(results, columns=['Combination', 'Price', 'Percentile'])
    return df

def calculate_combos_percentile_4_trades(directory_path, underlyer, target_date_str, percentile_years, combinations, SF):
    eligible_Call, eligible_Put = find_filenames(directory_path, underlyer, target_date_str, percentile_years, SF)
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    percentiles = []

    for combo in combinations:
        (tenor1, strike1, weight1, type1), (tenor2, strike2, weight2, type2) = combo[:2]
        (tenor3, strike3, weight3, type3), (tenor4, strike4, weight4, type4) = combo[2:]

        df_pair1 = load_files_and_calculate_combo_price(directory_path, eligible_Call, eligible_Put,
                                                        tenor1, strike1, weight1, tenor2, strike2, weight2, type1, type2)
        df_pair2 = load_files_and_calculate_combo_price(directory_path, eligible_Call, eligible_Put,
                                                        tenor3, strike3, weight3, tenor4, strike4, weight4, type3, type4)
        df_merged = pd.merge(df_pair1, df_pair2, on='Date', suffixes=('_pair1', '_pair2'))
        df_merged['Combined Spread'] = (
            df_merged[f'{type1}_{tenor1}_{strike1}'] * weight1 +
            df_merged[f'{type2}_{tenor2}_{strike2}'] * weight2 +
            df_merged[f'{type3}_{tenor3}_{strike3}'] * weight3 +
            df_merged[f'{type4}_{tenor4}_{strike4}'] * weight4
        )
        df_merged['Date'] = pd.to_datetime(df_merged['Date'], errors='coerce')
        df_merged.dropna(subset=['Date'], inplace=True)

        today_row = df_merged[df_merged['Date'] == target_date]
        if not today_row.empty:
            today_value = today_row['Combined Spread'].iloc[0]
            historical = df_merged[df_merged['Date'] < target_date]['Combined Spread']
            percentile = np.sum(historical < today_value) / len(historical) * 100 if not historical.empty else np.nan
            percentiles.append((combo, today_value, percentile))
        else:
            logging.info(f"No data for {target_date} for 4-legged combo {combo}")
            percentiles.append((combo, np.nan, np.nan))

    df = pd.DataFrame(percentiles, columns=['Combination', 'Price', 'Percentile'])
    return df


def parse_args():
    parser = argparse.ArgumentParser(description="Build option strategy CSVs for the latest available raw date.")
    parser.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker list. Defaults to app.py tickers.",
    )
    return parser.parse_args()



def process_ticker(root_path, ticker, date_str,
                   skew_combinations, straddle_combinations, strangles_combinations,
                   call_spread_combinations, put_spread_combinations,
                   call_ratio_combinations, put_ratio_combinations,
                   call_calendar_combinations, put_calendar_combinations,
                   iron_condor_combinations, iron_butterfly_combinations):
    directory_path = os.path.join(root_path, 'raw_percent')
    strategies_dir = os.path.join(root_path, 'strategies')
    os.makedirs(strategies_dir, exist_ok=True)

    try:
        skews = calculate_combos_percentile(directory_path, ticker, date_str, 2, skew_combinations, 'F')
        straddles = calculate_combos_percentile(directory_path, ticker, date_str, 2, straddle_combinations, 'F')
        strangles = calculate_combos_percentile(directory_path, ticker, date_str, 2, strangles_combinations, 'F')
        call_spreads = calculate_combos_percentile(directory_path, ticker, date_str, 2, call_spread_combinations, 'F')
        put_spreads = calculate_combos_percentile(directory_path, ticker, date_str, 2, put_spread_combinations, 'F')
        call_ratios = calculate_combos_percentile(directory_path, ticker, date_str, 2, call_ratio_combinations, 'F')
        put_ratios = calculate_combos_percentile(directory_path, ticker, date_str, 2, put_ratio_combinations, 'F')
        call_calendars = calculate_combos_percentile(directory_path, ticker, date_str, 2, call_calendar_combinations, 'S')
        put_calendars = calculate_combos_percentile(directory_path, ticker, date_str, 2, put_calendar_combinations, 'S')
        iron_condors = calculate_combos_percentile_4_trades(directory_path, ticker, date_str, 2, iron_condor_combinations, 'F')
        iron_butterflies = calculate_combos_percentile_4_trades(directory_path, ticker, date_str, 2, iron_butterfly_combinations, 'F')

        strategies = {
            'skews': skews,
            'straddles': straddles,
            'strangles': strangles,
            'call_spreads': call_spreads,
            'put_spreads': put_spreads,
            'call_ratios': call_ratios,
            'put_ratios': put_ratios,
            'call_calendars': call_calendars,
            'put_calendars': put_calendars,
            'iron_condors': iron_condors,
            'iron_butterflies': iron_butterflies
        }

        for strategy_name, df in strategies.items():
            save_path = os.path.join(strategies_dir, f'{ticker}_{strategy_name}_{date_str}.csv')
            df.to_csv(save_path, index=False)
            logging.info(f"Saved {strategy_name} for {ticker}")
    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")


if __name__ == '__main__':
    args = parse_args()
    selected_tickers = tickers
    if args.tickers:
        selected_tickers = [item.strip() for item in args.tickers.split(',') if item.strip()]

    if env == 'dev':
        root_path = DATA_ROOT
        log_dir = os.path.join(LOG_ROOT, 'Price', 'Options', 'Strategies')
    else:
        root_path = "/mnt/disks/local-ssd/Options"
        log_dir = "/mnt/disks/local-ssd/logs/Price/Options/Strategies"

    os.makedirs(log_dir, exist_ok=True)
    log_filename = datetime.now().strftime('%Y-%m-%d.log')
    log_filepath = os.path.join(log_dir, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')

    date_str = today_str
    #date_str = '2025-05-08'  # Example date string    

    # 1) Skew Trades
    skew_combinations = [  
        (('1w', 99.0, 1, 'Put'), ('1w', 101.0, 1, 'Call')), (('2w', 98.5, 1, 'Put'), ('2w', 101.5, 1, 'Call')), (('3w', 97.5, 1, 'Put'), ('3w', 102.5, 1, 'Call')),
        (('1m', 97.0, 1, 'Put'), ('1m', 103.0, 1, 'Call')), (('3m', 96.0, 1, 'Put'), ('3m', 104.0, 1, 'Call')),   
        (('6m', 94.0, 1, 'Put'), ('6m', 106.0, 1, 'Call')), (('1y', 90.0, 1, 'Put'), ('1y', 110.0, 1, 'Call'))
    ]

    # 2) Straddle at the money
    tenors = ['1w', '2w', '3w', '1m', '3m', '6m', '1y']
    weight_group_1 = 1  
    weight_group_2 = -1
    straddle_combinations = []
    strike = 100.0
    for tenor in tenors:
        straddle_combinations.append([(tenor, strike, weight_group_1, 'Call'), (tenor, strike, weight_group_2, 'Put')])

     # 3) Strangles out of the money
    strangles_combinations = [  
        (('1w', 98.5, 1, 'Put'), ('1w', 101.5, -1, 'Call')),(('2w', 98.0, 1, 'Put'), ('2w', 102.0, -1, 'Call')),  (('3w', 97.5, 1, 'Put'), ('3w', 102.5, -1, 'Call')),
        (('1m', 97.0, 1, 'Put'), ('1m', 103.0, -1, 'Call')), (('3m', 95.0, 1, 'Put'), ('3m', 105.0, -1, 'Call')),   
        (('6m', 92.0, 1, 'Put'), ('6m', 108.0, -1, 'Call')), (('1y', 88.0, 1, 'Put'), ('1y', 112.0, -1, 'Call'))
    ]

     # 4) Call Spreads
    call_spread_combinations = [  
        (('1w', 101.0, 1, 'Call'), ('1w', 102.0, 1, 'Call')), (('2w', 101.5, 1, 'Call'), ('2w', 102.5, 1, 'Call')),  (('3w', 102.0, 1, 'Call'), ('3w', 103.0, 1, 'Call')),
        (('1m', 103.0, 1, 'Call'), ('1m', 105.0, 1, 'Call')), (('3m', 104.0, 1, 'Call'), ('3m', 107.0, 1, 'Call')),   
        (('6m', 106.0, 1, 'Call'), ('6m', 110.0, 1, 'Call')), (('1y', 108.0, 1, 'Call'), ('1y', 114.0, 1, 'Call'))
    ]

    # 5) Put Spreads
    put_spread_combinations = [  
        (('1w', 99.0, 1, 'Put'), ('1w', 98.0, 1, 'Put')), (('2w', 98.5, 1, 'Put'), ('2w', 97.5, 1, 'Put')),  (('3w', 98.0, 1, 'Put'), ('3w', 97.0, 1, 'Put')),
        (('1m', 97.0, 1, 'Put'), ('1m', 95.0, 1, 'Put')), (('3m', 96.0, 1, 'Put'), ('3m', 93.0, 1, 'Put')),   
        (('6m', 94.0, 1, 'Put'), ('6m', 90.0, 1, 'Put')), (('1y', 92.0, 1, 'Put'), ('1y', 86.0, 1, 'Put'))
    ]

     # 6) Call ratio
    call_ratio_combinations = [  
        (('1w', 101.0, 1, 'Call'), ('1w', 102.0, 2, 'Call')), (('2w', 101.5, 1, 'Call'), ('2w', 102.5, 2, 'Call')),  (('3w', 102.0, 1, 'Call'), ('3w', 103.0, 2, 'Call')),
        (('1m', 103.0, 1, 'Call'), ('1m', 105.0, 2, 'Call')), (('3m', 104.0, 1, 'Call'), ('3m', 107.0, 2, 'Call')),   
        (('6m', 106.0, 1, 'Call'), ('6m', 110.0, 2, 'Call')), (('1y', 108.0, 1, 'Call'), ('1y', 114.0, 2, 'Call'))
    ]

    # 7) Put Ratio
    put_ratio_combinations = [  
        (('1w', 99.0, 1, 'Put'), ('1w', 98.0, 2, 'Put')), (('2w', 98.5, 1, 'Put'), ('2w', 97.5, 2, 'Put')),  (('3w', 98.0, 1, 'Put'), ('3w', 97.0, 2, 'Put')),
        (('1m', 97.0, 1, 'Put'), ('1m', 95.0, 2, 'Put')), (('3m', 96.0, 1, 'Put'), ('3m', 93.0, 2, 'Put')),   
        (('6m', 94.0, 1, 'Put'), ('6m', 90.0, 2, 'Put')), (('1y', 92.0, 1, 'Put'), ('1y', 86.0, 2, 'Put'))
    ]

    # 8) Call Calendar
    call_calendar_combinations = [
        (('1w', 101.0, -1, 'Call'), ('1m', 101.0, -1, 'Call')), (('1w', 101.0, -1, 'Call'), ('2m', 101.0, -1, 'Call')),  (('1w', 101.0, -1, 'Call'), ('3m', 101.0, -1, 'Call')),
        (('2w', 102.0, -1, 'Call'), ('1m', 102.0, -1, 'Call')), (('2w', 102.0, -1, 'Call'), ('2m', 102.0, -1, 'Call')), (('2w', 102.0, -1, 'Call'), ('3m', 102.0, -1, 'Call')),   
        (('1m', 103.0, -1, 'Call'), ('3m', 103.0, -1, 'Call')), (('1m', 104.0, -1, 'Call'), ('6m', 104.0, -1, 'Call')), #[('1m', 104.0, -1, 'Call'), ('9m', 107.0, -1, 'Call')],   
        (('3m', 106.0, -1, 'Call'), ('6m', 106.0, -1, 'Call')), (('3m', 106.0, -1, 'Call'), ('1y', 106.0, -1, 'Call'))
    ]

    # 9) Put Calendar
    put_calendar_combinations = [
        (('1w', 99.0, -1, 'Put'), ('1m', 99.0, -1, 'Put')), (('1w', 99.0, -1, 'Put'), ('2m', 99.0, -1, 'Put')),  (('1w', 99.0, -1, 'Put'), ('3m', 99.0, -1, 'Put')),
        (('2w', 98.0, -1, 'Put'), ('1m', 98.0, -1, 'Put')), (('2w', 98.0, -1, 'Put'), ('2m', 98.0, -1, 'Put')), (('2w', 98.0, -1, 'Put'), ('3m', 98.0, -1, 'Put')),   
        (('1m', 97.0, -1, 'Put'), ('3m', 97.0, -1, 'Put')), (('1m', 96.0, -1, 'Put'), ('6m', 96.0, -1, 'Put')), #[('1m', 104.0, -1, 'Put'), ('9m', 107.0, -1, 'Put')],   
        (('3m', 94.0, -1, 'Put'), ('6m', 94.0, -1, 'Put')), (('3m', 94.0, -1, 'Put'), ('1y', 94.0, -1, 'Put'))
    ]

    # 10) Iron Condor
    iron_condor_combinations = [
        (('1w', 99.0, 1, 'Put'), ('1w', 98.5, -1, 'Put'), ('1w', 101.0, 1, 'Call'), ('1w', 101.5, -1, 'Call')),  
        (('2w', 98.5, 1, 'Put'), ('2w', 98.0, -1, 'Put'), ('2w', 101.5, 1, 'Call'), ('2w', 102.0, -1, 'Call')),  
        (('3w', 98.0, 1, 'Put'), ('3w', 97.0, -1, 'Put'), ('3w', 102.0, 1, 'Call'), ('3w', 103.0, -1, 'Call')), 
        (('1m', 97.0, 1, 'Put'), ('1m', 95.0, -1, 'Put'), ('1m', 103.0, 1, 'Call'), ('1m', 105.0, -1, 'Call')),
        (('3m', 96.0, 1, 'Put'), ('3m', 94.0, -1, 'Put'), ('3m', 104.0, 1, 'Call'), ('3m', 106.0, -1, 'Call')),
        (('6m', 92.0, 1, 'Put'), ('6m', 90.0, -1, 'Put'), ('6m', 108.0, 1, 'Call'), ('6m', 110.0, -1, 'Call')),
    ]

    # 11) Iron Butterfly
    iron_butterfly_combinations = [
        (('1w', 100.0, 1, 'Put'), ('1w', 100.0, 1, 'Call'), ('1w', 98.5, -1, 'Put'), ('1w', 101.5, -1, 'Call')),  
        (('2w', 100.0, 1, 'Put'), ('2w', 100.0, 1, 'Call'), ('2w', 98.0, -1, 'Put'), ('2w', 102.0, -1, 'Call')),  
        (('3w', 100.0, 1, 'Put'), ('3w', 100.0, 1, 'Call'), ('3w', 97.5, -1, 'Put'), ('3w', 102.5, -1, 'Call')), 
        (('1m', 100.0, 1, 'Put'), ('1m', 100.0, 1, 'Call'), ('1m', 97.0, -1, 'Put'), ('1m', 103.0, -1, 'Call')),
        (('3m', 100.0, 1, 'Put'), ('3m', 100.0, 1, 'Call'), ('3m', 95.0, -1, 'Put'), ('3m', 105.0, -1, 'Call')),
        (('6m', 100.0, 1, 'Put'), ('6m', 100.0, 1, 'Call'), ('6m', 92.0, -1, 'Put'), ('6m', 108.0, -1, 'Call')),
    ]
    print(
        f"Option strategies refresh: {len(selected_tickers)} ticker(s) for {date_str}",
        flush=True,
    )
    for ticker in tqdm(selected_tickers, total=len(selected_tickers), desc="Processing tickers"):
        print(f"Starting ticker: {ticker}", flush=True)
        try:
            process_ticker(
                root_path, ticker, date_str,
                skew_combinations, straddle_combinations, strangles_combinations,
                call_spread_combinations, put_spread_combinations,
                call_ratio_combinations, put_ratio_combinations,
                call_calendar_combinations, put_calendar_combinations,
                iron_condor_combinations, iron_butterfly_combinations
            )
            print(f"Finished ticker: {ticker}", flush=True)
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            print(f"Error ticker: {ticker}: {e}", flush=True)
