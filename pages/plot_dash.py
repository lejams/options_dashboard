from dash import html, dcc
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from app import app, env, today_str, tickers, RAW_PERCENT_PATH
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go


tickers = sorted(tickers)
strikes = [f"{70 + 0.5 * i:.1f}" for i in range(121)]
tenors = ["1w", "2w", "3w", "1m", "2m", "3m", "6m", "1y", "2y"]


def axis_style(title_text):
    return dict(
        title=dict(
            text=title_text,
            font=dict(
                family="Arial, sans-serif",
                size=18,
                color="DarkSlateGrey",
            ),
        ),
        tickfont=dict(
            family="Arial, sans-serif",
            size=14,
            color="DarkSlateGrey",
        ),
    )


def figure_layout(title_text, yaxis_title):
    return go.Layout(
        title=dict(
            text=title_text,
            x=0.5,
            xanchor="center",
            font=dict(
                family="Arial, sans-serif",
                size=20,
                color="DarkSlateGrey",
            ),
        ),
        showlegend=False,
        yaxis=axis_style(yaxis_title),
        xaxis=axis_style("Date"),
    )


layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.H2(
                        "Plot option price and percentile for one option or a combo",
                        style={
                            "textAlign": "center",
                            "color": "#ffffff",
                            "background": "#333333",
                            "marginBottom": "30px",
                        },
                    ),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        id="asset-class-dropdown",
                        options=[{"label": i.split()[0], "value": i} for i in tickers],
                        value="SPX",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        id="price-vol-dropdown",
                        options=[{"label": i.split()[0], "value": i} for i in ["Price", "Vol"]],
                        value="Price",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        id="number-of-legs-dropdown",
                        options=[{"label": i, "value": i} for i in [1, 2]],
                        placeholder="Number of legs",
                        className="control control--dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="spot-forward-dropdown",
                        options=[
                            {"label": label, "value": value}
                            for label, value in zip(["Spot", "Forward"], ["S", "F"])
                        ],
                        value="S",
                        placeholder="Forward or Spot",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="type-dropdown",
                        options=[{"label": i, "value": i} for i in ["Call", "Put"]],
                        value="Call",
                        placeholder="Call or Put",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="tenor-dropdown",
                        options=[{"label": i, "value": i} for i in tenors],
                        value=None,
                        style={"display": "none"},
                        placeholder="Tenor",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="strike-dropdown",
                        value=None,
                        style={"display": "none"},
                        placeholder="Strike",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="weight-dropdown",
                        options=[{"label": i, "value": i} for i in [-2, -1.5, -1, -0.5, 0.5, 1, 1.5, 2]],
                        value=None,
                        placeholder="Weight",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        id="asset-class-dropdown-2",
                        options=[{"label": i, "value": i} for i in tickers],
                        value="SPX",
                        placeholder="Asset Classes",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="type-dropdown-2",
                        options=[{"label": i, "value": i} for i in ["Call", "Put"]],
                        value="Call",
                        placeholder="Call / Put",
                        className="control control--dropdown",
                        style={"display": "none"},
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="tenor-dropdown-2",
                        options=[{"label": i, "value": i} for i in tenors],
                        value="1m",
                        placeholder="Tenor",
                        className="control control-dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="strike-dropdown-2",
                        value="100.0",
                        placeholder="Strike:",
                        className="control control--dropdown",
                        multi=False,
                    ),
                    width=12,
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="weight-dropdown-2",
                        options=[{"label": i, "value": i} for i in [-2, -1.5, -1, -0.5, 0.5, 1, 1.5, 2]],
                        value=-1,
                        placeholder="Weight",
                        className="control control-dropdown",
                        style={"display": "none"},
                        multi=False,
                    ),
                    width=12,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Button("Confirm", id="confirm-button", n_clicks=0, className="btn btn-primary"),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Loading(
                        id="loading-1",
                        type="default",
                        children=[
                            dcc.Graph(id="option-graph"),
                            dcc.Graph(id="percentile-graph"),
                        ],
                        fullscreen=True,
                    ),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div(id="loading-output", className="typography--lead"), width=12)
            ]
        ),
    ],
    fluid=True,
)


@app.callback(
    [Output("strike-dropdown", "options"), Output("strike-dropdown", "style")],
    [Input("tenor-dropdown", "value"), Input("type-dropdown", "value")],
)
def update_strike_options(tenor_value, CP):
    tenors_1 = ["1w", "2w", "3w"]
    tenors_2 = ["1m", "2m", "3m"]
    tenors_3 = ["6m", "1y", "2y"]

    strike_options = []

    if CP and tenor_value:
        if CP == "Call":
            strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]
            strikes_2 = [round(x, 1) for x in range(100, 121)]
            strikes_3 = [round(x * 2, 1) for x in range(50, 66)]
        else:
            strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
            strikes_2 = [round(x, 1) for x in range(80, 101)]
            strikes_3 = [round(x * 2, 1) for x in range(35, 51)]

        if tenor_value in tenors_1:
            strike_options = [{"label": f"{i:.1f}", "value": f"{i:.1f}"} for i in strikes_1]
        elif tenor_value in tenors_2:
            strike_options = [{"label": f"{i:.1f}", "value": f"{i:.1f}"} for i in strikes_2]
        elif tenor_value in tenors_3:
            strike_options = [{"label": f"{i:.1f}", "value": f"{i:.1f}"} for i in strikes_3]

        return strike_options, {"display": "block"}

    return [], {"display": "none"}


@app.callback(
    Output("asset-class-dropdown-2", "style"),
    Output("type-dropdown-2", "style"),
    Output("tenor-dropdown-2", "style"),
    Output("weight-dropdown-2", "style"),
    Input("number-of-legs-dropdown", "value"),
)
def update_dropdown_display(value):
    hidden_style = {"display": "none"}
    show_style = {"display": "block"}
    if value == 2:
        return show_style, show_style, show_style, show_style
    return hidden_style, hidden_style, hidden_style, hidden_style


@app.callback(
    [Output("strike-dropdown-2", "options"), Output("strike-dropdown-2", "style")],
    [Input("tenor-dropdown-2", "value"), Input("type-dropdown-2", "value")],
)
def update_strike_options_2(tenor_value, CP):
    tenors_1 = ["1w", "2w", "3w"]
    tenors_2 = ["1m", "2m", "3m"]
    tenors_3 = ["6m", "1y", "2y"]

    strike_options = []

    if CP and tenor_value:
        if CP == "Call":
            strikes_1 = [round(x * 0.5, 1) for x in range(200, 221)]
            strikes_2 = [round(x, 1) for x in range(100, 121)]
            strikes_3 = [round(x * 2, 1) for x in range(50, 66)]
        else:
            strikes_1 = [round(x * 0.5, 1) for x in range(180, 201)]
            strikes_2 = [round(x, 1) for x in range(80, 101)]
            strikes_3 = [round(x * 2, 1) for x in range(35, 51)]

        if tenor_value in tenors_1:
            strike_options = [{"label": f"{i:.1f}", "value": f"{i:.1f}"} for i in strikes_1]
        elif tenor_value in tenors_2:
            strike_options = [{"label": f"{i:.1f}", "value": f"{i:.1f}"} for i in strikes_2]
        elif tenor_value in tenors_3:
            strike_options = [{"label": f"{i:.1f}", "value": f"{i:.1f}"} for i in strikes_3]

        return strike_options, {"display": "block"}

    return [], {"display": "none"}


def load_files_and_create_df(directory_path, file_prefix, target_date_str, percentile_years, option_param):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    one_year_ago = target_date - timedelta(days=percentile_years * 365)

    eligible_files = []
    files = os.listdir(directory_path)

    for filename in files:
        if filename.startswith(file_prefix) and filename.endswith(".csv"):
            file_date_str = filename.split("_")[-1].split(".")[0]
            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                if one_year_ago <= file_date <= target_date:
                    eligible_files.append((filename, file_date))
            except ValueError:
                continue

    data = []
    for filename, file_date in eligible_files:
        df = pd.read_csv(os.path.join(directory_path, filename))
        if "Tenor" not in df.columns:
            continue

        for tenor1, strike1 in option_param:
            strike_col = str(strike1)
            if tenor1 in df["Tenor"].values and strike_col in df.columns:
                value = df.loc[df["Tenor"] == tenor1, strike_col].values[0]
                data.append({"Date": file_date, "Value": value})

    result_df = pd.DataFrame(data, columns=["Date", "Value"])
    if result_df.empty:
        return pd.DataFrame(columns=["Date", "Value"])
    return result_df.sort_values(by="Date")


def load_data_and_calculate_rolling_percentiles(
    directory_path, file_prefix, target_date_str, percentile_years, n_years, option_param
):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    start_date = target_date - timedelta(days=365 * (percentile_years + n_years))

    relevant_data = pd.DataFrame()
    files = os.listdir(directory_path)

    for filename in files:
        if filename.startswith(file_prefix) and filename.endswith(".csv"):
            file_date_str = filename.split("_")[-1].split(".")[0]
            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                if start_date <= file_date <= target_date:
                    df = pd.read_csv(os.path.join(directory_path, filename))
                    tenor, strike = option_param
                    if "Tenor" in df.columns and tenor in df["Tenor"].values and strike in df.columns:
                        value_row = df.loc[df["Tenor"] == tenor, ["Tenor", strike]].copy()
                        value_row["Date"] = file_date
                        relevant_data = pd.concat([relevant_data, value_row], ignore_index=True)
            except ValueError:
                continue

    if relevant_data.empty:
        return pd.DataFrame(columns=["Date", "Tenor", "Value", "RollingPercentile"])

    relevant_data.rename(columns={strike: "Value"}, inplace=True)
    relevant_data["Date"] = pd.to_datetime(relevant_data["Date"])
    relevant_data.sort_values("Date", inplace=True)
    relevant_data.reset_index(drop=True, inplace=True)
    relevant_data.set_index("Date", inplace=True)

    rolling_window = f"{365 * percentile_years}D"
    relevant_data["RollingPercentile"] = relevant_data["Value"].rolling(
        window=rolling_window, min_periods=1
    ).apply(
        lambda x: np.sum(x < x.iloc[-1]) / len(x) * 100 if len(x) > 1 else np.nan,
        raw=False,
    )

    cutoff_date = target_date - timedelta(days=365 * n_years)
    final_data = relevant_data[relevant_data.index > cutoff_date].copy()
    final_data.reset_index(inplace=True)

    return final_data


def find_filenames(directory_path, underlyer, target_date_str, percentile_years, option_type, SF):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    one_year_ago = target_date - timedelta(days=percentile_years * 365)

    def is_file_eligible(filename, opt_type):
        file_date_str = filename.split("_")[-1].split(".")[0]
        try:
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
        except ValueError:
            return False

        conditions = [
            filename.startswith(underlyer),
            filename.endswith(".csv"),
            "vol" not in filename,
            opt_type in filename,
            ("spot" in filename) == (SF == "S"),
            one_year_ago < file_date <= target_date,
        ]
        return all(conditions)

    files = os.listdir(directory_path)

    eligible_files_calls = [f for f in files if is_file_eligible(f, "Call")]
    eligible_files_puts = [f for f in files if is_file_eligible(f, "Put")]

    if option_type == "Call":
        return eligible_files_calls
    return eligible_files_puts


def load_files_and_calculate_combo_price(
    directory_path,
    target_date_str,
    percentiles_years,
    SF,
    underlyer1,
    tenor1,
    strike1,
    weight1,
    type1,
    underlyer2,
    tenor2,
    strike2,
    weight2,
    type2,
):
    def process_files(underlyer, filenames, tenor, strike, option_type):
        data = []
        strike_col = str(strike)

        for filename in filenames:
            df = pd.read_csv(os.path.join(directory_path, filename))
            if "Tenor" not in df.columns or tenor not in df["Tenor"].values or strike_col not in df.columns:
                continue

            date = filename.split("_")[-1].split(".")[0]
            value = df.loc[df["Tenor"] == tenor, strike_col].values[0]
            data.append({"Date": date, f"{underlyer}_{option_type}_{tenor}_{strike}": value})

        return pd.DataFrame(data)

    files_leg1 = find_filenames(directory_path, underlyer1, target_date_str, percentiles_years, type1, SF)
    files_leg2 = find_filenames(directory_path, underlyer2, target_date_str, percentiles_years, type2, SF)

    df_leg1 = process_files(underlyer1, files_leg1, tenor1, strike1, type1)
    df_leg2 = process_files(underlyer2, files_leg2, tenor2, strike2, type2)

    if df_leg1.empty and df_leg2.empty:
        return pd.DataFrame(columns=["Date", "Spread"])

    if df_leg1.equals(df_leg2):
        df_final = df_leg1.copy()
    else:
        df_final = pd.merge(df_leg1, df_leg2, on="Date", how="outer")

    col1 = f"{underlyer1}_{type1}_{tenor1}_{strike1}"
    col2 = f"{underlyer2}_{type2}_{tenor2}_{strike2}"

    if col1 not in df_final.columns:
        df_final[col1] = np.nan
    if col2 not in df_final.columns:
        df_final[col2] = np.nan

    df_final["Spread"] = (df_final[col1] * weight1) + (df_final[col2] * weight2)
    df = df_final[["Date", "Spread"]].copy()
    df.dropna(inplace=True)
    df["Date"] = pd.to_datetime(df["Date"])
    df.sort_values(by="Date", inplace=True)

    return df

directory_path = RAW_PERCENT_PATH


@app.callback(
    Output("option-graph", "figure"),
    Output("percentile-graph", "figure"),
    [Input("confirm-button", "n_clicks")],
    [
        State("asset-class-dropdown", "value"),
        State("price-vol-dropdown", "value"),
        State("number-of-legs-dropdown", "value"),
        State("spot-forward-dropdown", "value"),
        State("type-dropdown", "value"),
        State("strike-dropdown", "value"),
        State("tenor-dropdown", "value"),
        State("weight-dropdown", "value"),
        State("asset-class-dropdown-2", "value"),
        State("type-dropdown-2", "value"),
        State("strike-dropdown-2", "value"),
        State("tenor-dropdown-2", "value"),
        State("weight-dropdown-2", "value"),
    ],
)
def update_graph(
    _,
    asset,
    pv,
    n_legs,
    SF,
    type_1,
    strike_1,
    tenor_1,
    weight_1,
    asset_2,
    type_2,
    strike_2,
    tenor_2,
    weight_2,
):
    if not n_legs:
        return go.Figure(), go.Figure()

    if SF == "S":
        if pv == "Price":
            file_prefix = f"{asset}_spot_{type_1}_option_percent"
        else:
            file_prefix = f"{asset}_spot_{type_1}_option_vol"
    else:
        if pv == "Price":
            file_prefix = f"{asset}_fwd_{type_1}_option_percent"
        else:
            file_prefix = f"{asset}_fwd_{type_1}_option_vol"

    df = pd.DataFrame(columns=["Date", "Value"])
    df_percentile = pd.DataFrame(columns=["Date", "RollingPercentile"])

    if n_legs == 2:
        df = load_files_and_calculate_combo_price(
            directory_path,
            today_str,
            2,
            SF,
            asset,
            tenor_1,
            strike_1,
            weight_1,
            type_1,
            asset_2,
            tenor_2,
            strike_2,
            weight_2,
            type_2,
        )
        if not df.empty:
            df["Value"] = df["Spread"]

        if weight_2 is not None and weight_2 > 0:
            title = (
                f"{weight_1} {asset.split()[0]} {type_1} {tenor_1} {strike_1} + "
                f"{weight_2} {asset_2.split()[0]} {type_2} {tenor_2} {strike_2}"
            )
        else:
            title = (
                f"{weight_1} {asset.split()[0]} {type_1} {tenor_1} {strike_1} - "
                f"{abs(weight_2)} {asset_2.split()[0]} {type_2} {tenor_2} {strike_2}"
            )
    else:
        df = load_files_and_create_df(
            directory_path,
            file_prefix,
            today_str,
            2,
            [(tenor_1, strike_1)],
        )
        df_percentile = load_data_and_calculate_rolling_percentiles(
            directory_path,
            file_prefix,
            today_str,
            2,
            2,
            (tenor_1, strike_1),
        )
        title = f"{asset.split()[0]} {type_1} {tenor_1} {strike_1}"

    if not df.empty:
        option_price_fig = go.Figure(
            data=[
                go.Scatter(
                    x=df["Date"],
                    y=df["Value"],
                    mode="lines",
                )
            ],
            layout=figure_layout(title, f"Option {pv} %"),
        )
    else:
        option_price_fig = go.Figure(layout=figure_layout(title if "title" in locals() else "No data", f"Option {pv} %"))

    if n_legs == 1 and not df_percentile.empty:
        option_percentile_fig = go.Figure(
            data=[
                go.Scatter(
                    x=df_percentile["Date"],
                    y=df_percentile["RollingPercentile"],
                    mode="lines",
                )
            ],
            layout=figure_layout(f"{title} 2y Percentile", "Option 2y Percentile %"),
        )
    else:
        option_percentile_fig = go.Figure()

    return option_price_fig, option_percentile_fig
