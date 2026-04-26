from dash import html, dcc, dash_table
import pandas as pd
import os
import json
import ast
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import dash_bootstrap_components as dbc

from app import app, tickers, STRATEGIES_MASTER_PATH
from dash.dependencies import Input, Output, State


tickers.sort()
path = STRATEGIES_MASTER_PATH

strategy_names = [
    "skews",
    "straddles",
    "strangles",
    "call_spreads",
    "put_spreads",
    "call_ratios",
    "put_ratios",
    "call_calendars",
    "put_calendars",
    "iron_condors",
    "iron_butterflies",
]

tenor_order = ["1w", "2w", "3w", "1m", "2m", "3m", "6m", "1y", "2y"]
tenor_rank = {tenor: i for i, tenor in enumerate(tenor_order)}


def read_parquet_safe(path: str) -> pd.DataFrame:
    table = pq.read_table(path)
    return table.to_pandas()


def parse_combination_json(value):
    if pd.isna(value):
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            try:
                return ast.literal_eval(value)
            except Exception:
                return []

    return []


def format_combination_old_style(combo):
    if not combo:
        return ""

    parts = []
    for leg in combo:
        if len(leg) >= 2:
            tenor = leg[0]
            strike = leg[1]
            try:
                strike_str = f"{float(strike):.1f}"
            except Exception:
                strike_str = str(strike)
            parts.append(f"{tenor} {strike_str}")

    return " | ".join(parts)


def combo_sort_key(combo):
    if not combo:
        return (999, 999.0, 999, 999.0, 999, 999.0, 999, 999.0)

    flattened = []
    for leg in combo:
        tenor = leg[0] if len(leg) > 0 else ""
        strike = leg[1] if len(leg) > 1 else 999.0

        tenor_idx = tenor_rank.get(tenor, 999)
        try:
            strike_val = float(strike)
        except Exception:
            strike_val = 999.0

        flattened.extend([tenor_idx, strike_val])

    while len(flattened) < 8:
        flattened.extend([999, 999.0])

    return tuple(flattened[:8])


def get_color_from_percentile(percentile, colorscale):
    if pd.isna(percentile):
        return "#f8fafc"

    percentile = max(0, min(100, float(percentile)))
    index = int((percentile / 100) * (len(colorscale) - 1))
    return colorscale[index]


def get_text_color_from_percentile(percentile):
    if pd.isna(percentile):
        return "#111827"

    percentile = float(percentile)
    if percentile <= 12 or percentile >= 88:
        return "#ffffff"
    return "#111827"


def title_from_strategy_name(strategy):
    return strategy.replace("_", " ").title()


def load_strategy_master(ticker):
    file_path = os.path.join(path, f"{ticker}_strategies_master.parquet")
    if not os.path.exists(file_path):
        return pd.DataFrame()

    df = read_parquet_safe(file_path)
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["percentile_2y"] = pd.to_numeric(df["percentile_2y"], errors="coerce")
    df["combination_label"] = df["combination_label"].astype(str)
    df["combination_json"] = df["combination_json"].astype(str)
    df["strategy_family"] = df["strategy_family"].astype(str)
    df["surface_type"] = df["surface_type"].astype(str)
    return df


def prepare_display_df(df):
    df = df.copy()

    df["parsed_combo"] = df["combination_json"].apply(parse_combination_json)
    df["Combination"] = df["parsed_combo"].apply(format_combination_old_style)
    df["sort_key"] = df["parsed_combo"].apply(combo_sort_key)

    df = df.sort_values("sort_key").reset_index(drop=True)

    df["Surface"] = df["surface_type"].astype(str)
    df["Price"] = df["price"].apply(lambda x: f"{x:.2f} %" if pd.notna(x) else "")
    df["Percentile"] = df["percentile_2y"].apply(lambda x: f"{x:.2f} %" if pd.notna(x) else "")
    df["Percentile Numeric"] = pd.to_numeric(df["percentile_2y"], errors="coerce")

    return df


def build_table_component(strategy):
    return html.Div(
        [
            html.H3(id=f"title_{strategy}"),
            dash_table.DataTable(
                id=f"table_{strategy}",
                columns=[
                    {"name": "Surface", "id": "Surface"},
                    {"name": "Combination", "id": "Combination"},
                    {"name": "Price", "id": "Price"},
                    {"name": "Percentile", "id": "Percentile"},
                ],
                data=[],
                style_cell={
                    "textAlign": "center",
                    "fontFamily": "monospace",
                    "fontSize": "15px",
                    "padding": "8px",
                },
                style_header={
                    "backgroundColor": "rgb(230, 230, 230)",
                    "fontWeight": "bold",
                    "fontFamily": "monospace",
                    "fontSize": "15px",
                },
                style_table={"overflowX": "auto"},
                row_selectable="single",
                selected_rows=[],
            ),
        ],
        className="centered-table",
    )


def build_strategy_table_payload(df_all, strategy):
    df = df_all[df_all["strategy_family"] == strategy].copy()
    title = title_from_strategy_name(strategy)

    if df.empty:
        return title, [], []

    latest_date = df["date"].max()
    df = df[df["date"] == latest_date].copy()

    display_df = prepare_display_df(df)
    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"
    title = f"{title_from_strategy_name(strategy)} {latest_date_str}"

    colorscale = [
        "#b91c1c",
        "#dc4f43",
        "#ec7c6d",
        "#f3aaa0",
        "#f9d8d2",
        "#f8fafc",
        "#d7eaf3",
        "#acd4e6",
        "#75b6d7",
        "#3f91c1",
        "#1f6fa8",
    ]

    style_data_conditional = [
        {
            "if": {"column_id": "Percentile", "row_index": i},
            "backgroundColor": get_color_from_percentile(row["Percentile Numeric"], colorscale),
            "color": get_text_color_from_percentile(row["Percentile Numeric"]),
            "fontWeight": "700",
        }
        for i, row in display_df.iterrows()
    ] + [
        {"if": {"column_id": "Surface"}, "minWidth": "90px", "width": "90px", "maxWidth": "90px"},
        {"if": {"column_id": "Combination"}, "minWidth": "260px", "width": "260px", "maxWidth": "260px"},
        {"if": {"column_id": "Price"}, "minWidth": "140px", "width": "140px", "maxWidth": "140px"},
        {"if": {"column_id": "Percentile"}, "minWidth": "140px", "width": "140px", "maxWidth": "140px"},
    ]

    table_df = display_df[
        [
            "Surface",
            "Combination",
            "Price",
            "Percentile",
            "strategy_family",
            "combination_label",
            "combination_json",
        ]
    ].copy()

    for col in table_df.columns:
        table_df[col] = table_df[col].astype(str)

    data = table_df.to_dict("records")
    return title, data, style_data_conditional


def make_line_figure(df, x_col, y_col, title, yaxis_title):
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=title, height=320)
        return fig

    fig.add_trace(go.Scatter(x=df[x_col], y=df[y_col], mode="lines"))
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=yaxis_title,
        height=320,
    )
    return fig


def first_difference(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").diff()


layout = html.Div(
    [
        dcc.Dropdown(
            id="asset-class-dropdown",
            options=[{"label": i.split()[0], "value": i} for i in tickers],
            value="SPX",
            placeholder="Asset Classes",
            className="control control-dropdown",
            multi=False,
        ),
        html.Div(
            id="table-container",
            children=[build_table_component(strategy) for strategy in strategy_names],
            style={"columnCount": 2, "columnGap": "80px", "padding": "20px"},
        ),
        html.Div(id="strategy-detail-container"),
    ]
)


for strategy in strategy_names:
    def make_callback(strategy_name):
        @app.callback(
            Output(f"title_{strategy_name}", "children"),
            Output(f"table_{strategy_name}", "data"),
            Output(f"table_{strategy_name}", "style_data_conditional"),
            Input("asset-class-dropdown", "value"),
        )
        def update_single_table(selected_ticker):
            df_all = load_strategy_master(selected_ticker)
            title, data, style_data_conditional = build_strategy_table_payload(df_all, strategy_name)
            return title, data, style_data_conditional

        return update_single_table

    make_callback(strategy)


@app.callback(
    Output("strategy-detail-container", "children"),
    Input("asset-class-dropdown", "value"),
    *[Input(f"table_{strategy}", "selected_rows") for strategy in strategy_names],
    *[State(f"table_{strategy}", "data") for strategy in strategy_names],
)
def update_strategy_detail(selected_ticker, *args):
    selected_rows_list = args[: len(strategy_names)]
    data_list = args[len(strategy_names) :]

    selected_record = None

    for selected_rows, table_data in zip(selected_rows_list, data_list):
        if selected_rows and table_data:
            idx = selected_rows[0]
            if 0 <= idx < len(table_data):
                selected_record = table_data[idx]
                break

    if selected_record is None:
        return html.Div()

    strategy_family = str(selected_record["strategy_family"])
    combination_label = str(selected_record["combination_label"])
    combination_json = str(selected_record["combination_json"])

    df_all = load_strategy_master(selected_ticker)
    if df_all.empty:
        return html.Div("No strategy history available.")

    df_hist = df_all[
        (df_all["strategy_family"].astype(str) == strategy_family)
        & (df_all["combination_label"].astype(str) == combination_label)
        & (df_all["combination_json"].astype(str) == combination_json)
    ].copy()

    if df_hist.empty:
        return html.Div("No history found for selected strategy.")

    df_hist = df_hist.sort_values("date").reset_index(drop=True)
    df_hist["price_diff"] = first_difference(df_hist["price"])
    df_hist["percentile_diff"] = first_difference(df_hist["percentile_2y"])

    latest_row = df_hist.iloc[-1]
    latest_date = latest_row["date"]
    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"

    parsed_combo = parse_combination_json(combination_json)
    display_combination = format_combination_old_style(parsed_combo)

    summary = dbc.Card(
        dbc.CardBody(
            [
                html.H3("Selected Strategy Details"),
                html.H5(title_from_strategy_name(strategy_family)),
                html.P(f"Combination: {display_combination}"),
                html.P(f"Surface: {latest_row['surface_type']}"),
                html.P(f"Latest date: {latest_date_str}"),
                html.P(f"Current price: {latest_row['price']:.4f} %" if pd.notna(latest_row['price']) else "Current price: N/A"),
                html.P(
                    f"Current percentile: {latest_row['percentile_2y']:.2f}"
                    if pd.notna(latest_row["percentile_2y"])
                    else "Current percentile: N/A"
                ),
            ]
        ),
        style={"marginBottom": "20px"},
    )

    price_fig = make_line_figure(df_hist, "date", "price", "Strategy Price History", "Price %")
    percentile_fig = make_line_figure(df_hist, "date", "percentile_2y", "Strategy Percentile History", "Percentile")
    price_diff_fig = make_line_figure(
        df_hist.dropna(subset=["price_diff"]),
        "date",
        "price_diff",
        "First Difference of Price",
        "Delta Price",
    )
    percentile_diff_fig = make_line_figure(
        df_hist.dropna(subset=["percentile_diff"]),
        "date",
        "percentile_diff",
        "First Difference of Percentile",
        "Delta Percentile",
    )

    return html.Div(
        [
            html.Hr(),
            summary,
            dbc.Row([dbc.Col(dcc.Graph(figure=price_fig), width=12)]),
            dbc.Row([dbc.Col(dcc.Graph(figure=percentile_fig), width=12)]),
            dbc.Row([dbc.Col(dcc.Graph(figure=price_diff_fig), width=12)]),
            dbc.Row([dbc.Col(dcc.Graph(figure=percentile_diff_fig), width=12)]),
        ]
    )
