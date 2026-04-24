from dash import Dash
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
import os

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.config.suppress_callback_exceptions = True
server = app.server

env = os.getenv("OPTIONS_ENV", "dev").strip().lower()
DATA_ROOT = os.getenv("OPTIONS_DATA_ROOT", "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322")
RAW_PERCENT_PATH = os.path.join(DATA_ROOT, "raw_percent")
PERCENTILE_PATH = os.path.join(DATA_ROOT, "percentile")
PERCENTILE_MASTER_PATH = os.path.join(DATA_ROOT, "percentile_master")
STRATEGIES_PATH = os.path.join(DATA_ROOT, "strategies")
STRATEGIES_MASTER_PATH = os.path.join(DATA_ROOT, "strategies_master")
MASTER_PATH = os.path.join(DATA_ROOT, "master")
LOG_ROOT = os.path.join(DATA_ROOT, "logs")


def fallback_market_date(now):
    market_date = now
    if now.weekday() == 5:
        market_date = now - timedelta(days=1)
    elif now.weekday() == 6:
        market_date = now - timedelta(days=2)
    elif now.weekday() == 0:
        if now.hour < 21 or (now.hour == 21 and now.minute < 45):
            market_date = now - timedelta(days=3)
    elif now.weekday() in [1, 2, 3, 4]:
        if now.hour < 21 or (now.hour == 21 and now.minute < 45):
            market_date = now - timedelta(days=1)
    return market_date


def latest_available_raw_date(raw_path):
    latest = None
    if not os.path.exists(raw_path):
        return None
    for filename in os.listdir(raw_path):
        if not filename.endswith('.csv'):
            continue
        try:
            file_date = datetime.strptime(filename.split('_')[-1].split('.')[0], '%Y-%m-%d')
        except ValueError:
            continue
        if latest is None or file_date > latest:
            latest = file_date
    return latest


now = datetime.now()
raw_latest = latest_available_raw_date(RAW_PERCENT_PATH)
today = raw_latest if raw_latest is not None else fallback_market_date(now)
today_str = today.strftime("%Y-%m-%d")

if today.weekday() == 0:
    yesterday = today - timedelta(days=3)
else:
    yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")


tickers = ['SPX', 'QQQ US', 'DIA US', 'IWM US', 'GLD US', 'XLF US', 'XLE US', 'XLC US',
            'XLP US', 'XLV US', 'IYR US', 'HYG US', 'EEM US', 'FXI US', 'EWZ US', 'EWI US',
            'TLT UQ', 'XLK UP','XHB UP', 'SLV UP', 'USO UP', 'SX5E', 'NKY','NDX', 'RTY',
            'DAX', 'UKX', 'SMI', 'HSCEI', 'HSI', 'KOSPI2', 'AS51'
            ]
problematic_tickers = ['AS51', 'DAX', 'HSCEI', 'HSI', 'KOSPI2', 'SMI', 'UKX']
tickers = [t for t in tickers if t not in problematic_tickers]
