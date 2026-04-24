from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import os
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from app import app, tickers, DATA_ROOT


tickers.sort()

MASTER_DIR = os.path.join(DATA_ROOT, "master")
TENOR_ORDER = ["1w", "2w", "3w", "1m", "2m", "3m", "6m", "1y", "2y"]
TENOR_RANK = {tenor: i for i, tenor in enumerate(TENOR_ORDER)}
MASTER_COLUMNS = ["date", "surface_type", "tenor", "strike_pct", "vol"]
_VOL_SURFACE_CACHE = {}


def read_parquet_safe(path: str, columns: list[str] | None = None) -> pd.DataFrame:
    table = pq.read_table(path, columns=columns)
    return table.to_pandas()


def load_master_for_ticker(ticker: str, surface_type: str) -> pd.DataFrame:
    path = os.path.join(MASTER_DIR, f"{ticker}_master.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()

    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table(
        columns=MASTER_COLUMNS,
        filter=ds.field("surface_type") == surface_type,
    )
    df = table.to_pandas()
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["strike_pct"] = pd.to_numeric(df["strike_pct"], errors="coerce")
    df["vol"] = pd.to_numeric(df["vol"], errors="coerce")
    df["tenor"] = df["tenor"].astype(str)

    df = df.dropna(subset=["date", "tenor", "strike_pct", "vol"])
    return df


def build_latest_vol_surface(ticker: str, surface_type: str) -> tuple[pd.DataFrame, pd.Timestamp]:
    cache_key = (ticker, surface_type)
    if cache_key in _VOL_SURFACE_CACHE:
        return _VOL_SURFACE_CACHE[cache_key]

    df = load_master_for_ticker(ticker, surface_type)
    if df.empty:
        return pd.DataFrame(), pd.NaT

    latest_date = df["date"].max()
    df = df[df["date"] == latest_date].copy()

    # collapse possible call/put duplicates by averaging vol on same node
    df = (
        df.groupby(["tenor", "strike_pct"], as_index=False)["vol"]
        .mean()
        .copy()
    )

    df["tenor_rank"] = df["tenor"].map(TENOR_RANK)
    df = df.dropna(subset=["tenor_rank"]).sort_values(["tenor_rank", "strike_pct"]).reset_index(drop=True)

    result = (df, latest_date)
    _VOL_SURFACE_CACHE[cache_key] = result
    return result


def nearest_available_strikes(df_surface: pd.DataFrame, targets: list[float]) -> list[float]:
    available = sorted(df_surface["strike_pct"].dropna().unique().tolist())
    if not available:
        return []

    selected = []
    for target in targets:
        nearest = min(available, key=lambda x: abs(x - target))
        if nearest not in selected:
            selected.append(nearest)
    return selected


def make_smile_figure(df_surface: pd.DataFrame, latest_date: pd.Timestamp, ticker: str, surface_type: str):
    fig = go.Figure()

    if df_surface.empty:
        fig.update_layout(title="No vol surface available", height=500)
        return fig

    for tenor in TENOR_ORDER:
        sub = df_surface[df_surface["tenor"] == tenor].copy()
        if sub.empty:
            continue

        fig.add_trace(
            go.Scatter(
                x=sub["strike_pct"],
                y=sub["vol"],
                mode="lines+markers",
                name=tenor,
            )
        )

    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"

    fig.update_layout(
        title=f"{ticker.split()[0]} {surface_type} vol smile by tenor<br><sup>Latest date: {latest_date_str}</sup>",
        xaxis_title="Strike %",
        yaxis_title="Vol",
        height=520,
        legend_title="Tenor",
        template="plotly_white",
    )
    return fig


def make_term_structure_figure(df_surface: pd.DataFrame, latest_date: pd.Timestamp, ticker: str, surface_type: str):
    fig = go.Figure()

    if df_surface.empty:
        fig.update_layout(title="No term structure available", height=500)
        return fig

    selected_strikes = nearest_available_strikes(df_surface, [90.0, 95.0, 100.0, 105.0, 110.0])

    for strike in selected_strikes:
        sub = df_surface[df_surface["strike_pct"] == strike].copy()
        if sub.empty:
            continue

        sub["tenor_rank"] = sub["tenor"].map(TENOR_RANK)
        sub = sub.sort_values("tenor_rank")

        fig.add_trace(
            go.Scatter(
                x=sub["tenor"],
                y=sub["vol"],
                mode="lines+markers",
                name=f"{strike:.1f}",
            )
        )

    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"

    fig.update_layout(
        title=f"{ticker.split()[0]} {surface_type} term structure by strike<br><sup>Latest date: {latest_date_str}</sup>",
        xaxis_title="Tenor",
        yaxis_title="Vol",
        height=520,
        legend_title="Strike %",
        template="plotly_white",
    )
    return fig


def make_heatmap_figure(df_surface: pd.DataFrame, latest_date: pd.Timestamp, ticker: str, surface_type: str):
    fig = go.Figure()

    if df_surface.empty:
        fig.update_layout(title="No heatmap available", height=520)
        return fig

    pivot = df_surface.pivot(index="tenor", columns="strike_pct", values="vol")
    tenor_index = [tenor for tenor in TENOR_ORDER if tenor in pivot.index]
    pivot = pivot.reindex(tenor_index)

    fig.add_trace(
        go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale="RdBu_r",
            colorbar=dict(title="Vol"),
            hovertemplate="Tenor=%{y}<br>Strike=%{x}<br>Vol=%{z:.4f}<extra></extra>",
        )
    )

    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"

    fig.update_layout(
        title=f"{ticker.split()[0]} {surface_type} heatmap<br><sup>Latest date: {latest_date_str}</sup>",
        xaxis_title="Strike %",
        yaxis_title="Tenor",
        height=520,
        template="plotly_white",
    )
    return fig


layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H2("Vol Surface", style={"marginBottom": "20px"}),
                        dcc.Dropdown(
                            id="vol-ticker-dropdown",
                            options=[{"label": t.split()[0], "value": t} for t in tickers],
                            value="SPX" if "SPX" in tickers else (tickers[0] if tickers else None),
                            placeholder="Ticker",
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=6,
                ),
                dbc.Col(
                    [
                        html.Div(style={"height": "38px"}),
                        dcc.Dropdown(
                            id="vol-surface-dropdown",
                            options=[
                                {"label": "Spot", "value": "spot"},
                                {"label": "Forward", "value": "fwd"},
                            ],
                            value="fwd",
                            placeholder="Surface type",
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=6,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Tabs(
                        id="vol-tabs",
                        value="smile",
                        children=[
                            dcc.Tab(label="Smile", value="smile"),
                            dcc.Tab(label="Term Structure", value="term"),
                            dcc.Tab(label="Heatmap", value="heatmap"),
                        ],
                    ),
                    width=12,
                )
            ],
            style={"marginTop": "20px"},
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Loading(
                        type="default",
                        children=dcc.Graph(id="vol-main-graph"),
                    ),
                    width=12,
                )
            ],
            style={"marginTop": "20px"},
        ),
    ],
    fluid=True,
)


@app.callback(
    Output("vol-main-graph", "figure"),
    Input("vol-ticker-dropdown", "value"),
    Input("vol-surface-dropdown", "value"),
    Input("vol-tabs", "value"),
)
def update_vol_graph(ticker, surface_type, tab_value):
    if not ticker or not surface_type:
        fig = go.Figure()
        fig.update_layout(title="Please select a ticker and a surface type", height=500)
        return fig

    df_surface, latest_date = build_latest_vol_surface(ticker, surface_type)

    if tab_value == "smile":
        return make_smile_figure(df_surface, latest_date, ticker, surface_type)

    if tab_value == "term":
        return make_term_structure_figure(df_surface, latest_date, ticker, surface_type)

    return make_heatmap_figure(df_surface, latest_date, ticker, surface_type)
