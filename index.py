from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
import pyarrow.parquet as pq
import os
import numpy as np

import pages.option_strategies_dash
import pages.plot_dash
import pages.solver_dash
import pages.vol_dash

from app import app, env, tickers, server, PERCENTILE_MASTER_PATH


tickers.sort()


navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Strategies", href="/option-strategies")),
        dbc.NavItem(dbc.NavLink("Solver", href="/option-solver")),
        dbc.NavItem(dbc.NavLink("Graph", href="/option-graph")),
        dbc.NavItem(dbc.NavLink("Vol", href="/vol")),
        dbc.NavItem(dbc.NavLink("Main", href="/")),
    ],
    brand="Option Strategies",
    brand_href="#",
    color="primary",
    dark=True,
    className="custom-navbar",
)


main_page_layout = html.Div(
    [
        html.H1(
            "Welcome to the Options Dashboard",
            style={
                "textAlign": "center",
                "color": "#ffffff",
                "background": "#333333",
                "marginBottom": "30px",
            },
        ),
        dcc.Dropdown(
            id="asset-class-dropdown",
            options=[{"label": i.split()[0], "value": i} for i in tickers],
            value="SPX",
            placeholder="Asset Classes",
            className="control control-dropdown",
            multi=False,
        ),
        dcc.Dropdown(
            id="spot-fwd-dropdown",
            options=[{"label": i, "value": i} for i in ["Spot", "Forward"]],
            value="Forward",
            placeholder="Spot/Forward",
            className="control control-dropdown",
            multi=False,
        ),
        dcc.Dropdown(
            id="duration-dropdown",
            options=[{"label": i, "value": i} for i in ["Short", "Medium", "Long"]],
            value="Short",
            placeholder="Duration",
            className="control control-dropdown",
            multi=False,
        ),
        html.Div(
            [
                dcc.Graph(id="heatmap-graph-spot-p"),
                dcc.Graph(id="heatmap-graph-spot-c"),
            ],
            className="centered-graph",
        ),
        html.Div(id="on-load", style={"display": "none"}),
    ],
    style={
        "maxWidth": "1200px",
        "margin": "40px auto",
        "padding": "20px",
        "boxShadow": "0px 4px 8px rgba(0,0,0,0.5)",
        "borderRadius": "8px",
        "backgroundColor": "#ecf0f1",
    },
)


def read_parquet_safe(path: str) -> pd.DataFrame:
    table = pq.read_table(path)
    return table.to_pandas()


def process_df(df, duration):
    if duration == "Short":
        return df.iloc[:3, :].dropna(axis=1, how="all").iloc[::-1]
    elif duration == "Medium":
        return df.iloc[3:6, :].dropna(axis=1, how="all").iloc[::-1]
    else:
        return df.iloc[6:, :].dropna(axis=1, how="all").iloc[::-1]


def build_surface(df_asset: pd.DataFrame, surface_type: str, option_type: str):
    df = df_asset[
        (df_asset["surface_type"] == surface_type)
        & (df_asset["option_type"] == option_type)
    ].copy()

    if df.empty:
        return pd.DataFrame(), pd.NaT

    latest_date = df["date"].max()
    df = df[df["date"] == latest_date].copy()

    pivot = (
        df.pivot_table(
            index="tenor",
            columns="strike_pct",
            values="percentile_2y",
            aggfunc="mean",
        )
        .sort_index()
        .sort_index(axis=1)
    )

    tenor_order = ["1w", "2w", "3w", "1m", "2m", "3m", "6m", "1y", "2y"]
    pivot = pivot.reindex([t for t in tenor_order if t in pivot.index])

    return pivot, latest_date


@app.callback(
    Output("heatmap-graph-spot-c", "figure"),
    Output("heatmap-graph-spot-p", "figure"),
    Input("on-load", "children"),
    Input("asset-class-dropdown", "value"),
    Input("spot-fwd-dropdown", "value"),
    Input("duration-dropdown", "value"),
)
def update_heatmap(_, asset, sf_label, duration):
    file_path = os.path.join(PERCENTILE_MASTER_PATH, f"{asset}_percentile_master.parquet")

    if not os.path.exists(file_path):
        fig = go.Figure()
        fig.update_layout(title=f"Missing percentile master file for {asset}")
        return fig, fig

    df_asset = read_parquet_safe(file_path)
    df_asset["date"] = pd.to_datetime(df_asset["date"], errors="coerce")

    surface_type = "spot" if sf_label == "Spot" else "fwd"

    calls, latest_date_c = build_surface(df_asset, surface_type, "Call")
    puts, latest_date_p = build_surface(df_asset, surface_type, "Put")

    if calls.empty:
        fig_calls = go.Figure()
        fig_calls.update_layout(title=f"No call percentile data for {asset} / {surface_type}")
    else:
        calls = process_df(calls, duration)
        latest_date_str = latest_date_c.strftime("%Y-%m-%d") if pd.notna(latest_date_c) else "N/A"

        fig_calls = go.Figure(
            data=go.Heatmap(
                z=calls.values,
                x=np.array(calls.columns),
                y=np.array(calls.index),
                colorscale=list(
                    reversed(
                        [
                            [0, "red"],
                            [0.5, "white"],
                            [1, "lightblue"],
                        ]
                    )
                ),
                text=calls.values,
                texttemplate="%{text:.1f}",
                hoverinfo="none",
                colorbar=dict(title="Percentile Rank (%)"),
            )
        )

        fig_calls.update_layout(
            xaxis_title="Calls",
            yaxis_title="Tenor",
            xaxis=dict(
                tickmode="array",
                tickvals=calls.columns,
                ticktext=calls.columns,
                tickangle=-45,
            ),
            autosize=True,
            xaxis_side="top",
            width=1000,
            height=600,
            annotations=[
                dict(
                    text=f"Option Prices Percentile ({asset}) {latest_date_str}",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=-0.1,
                    xanchor="center",
                    yanchor="top",
                    font=dict(size=16, color="black"),
                )
            ],
        )

    if puts.empty:
        fig_puts = go.Figure()
        fig_puts.update_layout(title=f"No put percentile data for {asset} / {surface_type}")
    else:
        puts = process_df(puts, duration)
        latest_date_str = latest_date_p.strftime("%Y-%m-%d") if pd.notna(latest_date_p) else "N/A"

        fig_puts = go.Figure(
            data=go.Heatmap(
                z=puts.values,
                x=np.array(puts.columns),
                y=np.array(puts.index),
                colorscale=list(
                    reversed(
                        [
                            [0, "red"],
                            [0.5, "white"],
                            [1, "lightblue"],
                        ]
                    )
                ),
                text=puts.values,
                texttemplate="%{text:.1f}",
                hoverinfo="none",
                colorbar=dict(title="Percentile Rank (%)"),
            )
        )

        fig_puts.update_layout(
            xaxis_title="Puts",
            yaxis_title="Tenor",
            xaxis=dict(
                tickmode="array",
                tickvals=puts.columns,
                ticktext=puts.columns,
                tickangle=-45,
            ),
            autosize=True,
            xaxis_side="top",
            width=1000,
            height=600,
            annotations=[
                dict(
                    text=f"Option Prices Percentile ({asset}) {latest_date_str}",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=-0.1,
                    xanchor="center",
                    yanchor="top",
                    font=dict(size=16, color="black"),
                )
            ],
        )

    return fig_calls, fig_puts


app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        navbar,
        html.Div(id="page-content"),
    ]
)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname):
    if pathname == "/option-strategies":
        return pages.option_strategies_dash.layout
    elif pathname == "/option-solver":
        return pages.solver_dash.layout
    elif pathname == "/option-graph":
        return pages.plot_dash.layout
    elif pathname == "/vol":
        return pages.vol_dash.layout
    else:
        return main_page_layout


if __name__ == "__main__":
    host = os.getenv("OPTIONS_HOST", "0.0.0.0")
    port = int(os.getenv("OPTIONS_PORT", "8057"))
    app.run(debug=(env == "dev"), host=host, port=port)
