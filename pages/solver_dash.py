from dash import dcc, html
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go

from app import app, tickers
from solver_engine import build_solver_matrix, get_combo_detail


tickers.sort()
tenors = ["1w", "2w", "3w", "1m", "2m", "3m", "6m", "1y", "2y"]


layout = dbc.Container(
    [
        dbc.Row(
            [
                html.H2(
                    "Solve option percentiles for combinations of strikes",
                    style={
                        "textAlign": "center",
                        "color": "#ffffff",
                        "background": "#333333",
                        "marginBottom": "30px",
                    },
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Dropdown(
                                id="ticker-dropdown",
                                options=[{"label": i.split()[0], "value": i} for i in tickers],
                                value=None,
                                placeholder="Ticker",
                                className="control control--dropdown",
                                multi=False,
                            ),
                            width=12,
                        ),
                    ]
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="spot-forward-dropdown",
                        options=[
                            {"label": label, "value": value}
                            for label, value in zip(["Spot", "Forward"], ["S", "F"])
                        ],
                        placeholder="Forward or Spot?",
                        value=None,
                        style={"display": "none"},
                        className="control control--dropdown",
                        multi=False,
                    ),
                    width=6,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="tenor-dropdown",
                            options=[{"label": i, "value": i} for i in tenors],
                            placeholder="Tenor",
                            value=None,
                            style={"display": "none"},
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="type-dropdown",
                            options=[{"label": i, "value": i} for i in ["Call", "Put"]],
                            value=None,
                            placeholder="Calls or Puts?",
                            style={"display": "none"},
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="min-strike-dropdown",
                            placeholder="Min Strike",
                            value=None,
                            style={"display": "none"},
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="max-strike-dropdown",
                            placeholder="Max Strike",
                            value=None,
                            style={"display": "none"},
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="weight-dropdown",
                            options=[{"label": i, "value": i} for i in [-2, -1, 1, 2]],
                            value=None,
                            style={"display": "none"},
                            placeholder="Weight",
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="tenor-dpdown-2",
                            options=[{"label": i, "value": i} for i in tenors],
                            placeholder="Tenor",
                            value=None,
                            style={"display": "none"},
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="type-dpdown-2",
                            options=[{"label": i, "value": i} for i in ["Call", "Put"]],
                            value=None,
                            style={"display": "none"},
                            placeholder="Calls or Puts?",
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="min-strike-dropdown-2",
                            placeholder="Strike",
                            value=None,
                            style={"display": "none"},
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="max-strike-dropdown-2",
                            placeholder="Strike",
                            style={"display": "none"},
                            value=None,
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="weight-dpdown-2",
                            options=[{"label": i, "value": i} for i in [-2, -1, 1, 2]],
                            value=None,
                            style={"display": "none"},
                            placeholder="Weight",
                            className="control control--dropdown",
                            multi=False,
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Button(
                            "Calculate",
                            id="confirm-button-solver",
                            n_clicks=0,
                            className="btn btn-primary",
                        )
                    ],
                    lg=3,
                    md=4,
                    sm=6,
                    xs=12,
                    className="centered",
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Loading(
                            id="loading",
                            type="default",
                            children=[dcc.Graph(id="graph-output")],
                            fullscreen=True,
                        ),
                    ],
                    width=12,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.H4("Selected Combo Details", style={"marginTop": "30px"}),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div(id="solver-detail-summary"), width=12),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="solver-detail-price-graph"), width=12),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="solver-detail-percentile-graph"), width=12),
            ]
        ),
    ],
    fluid=True,
)


@app.callback(
    Output("spot-forward-dropdown", "style"),
    Input("ticker-dropdown", "value"),
)
def show_spot_forward_dropdown(asset_value):
    if asset_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    Output("tenor-dropdown", "style"),
    Input("spot-forward-dropdown", "value"),
)
def show_tenor_dropdown(sf_value):
    if sf_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    Output("type-dropdown", "style"),
    Input("spot-forward-dropdown", "value"),
)
def show_type_dropdown(spot_forward_value):
    if spot_forward_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    [
        Output("min-strike-dropdown", "options"),
        Output("min-strike-dropdown", "style"),
        Output("max-strike-dropdown", "options"),
        Output("max-strike-dropdown", "style"),
    ],
    [
        Input("tenor-dropdown", "value"),
        Input("type-dropdown", "value"),
    ],
)
def update_strike_options(tenor_value, cp_value):
    tenors_1 = ["1w", "2w", "3w"]
    tenors_2 = ["1m", "2m", "3m"]
    tenors_3 = ["6m", "1y", "2y"]

    strike_options = []

    if cp_value and tenor_value:
        if cp_value == "Call":
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

        return strike_options, {"display": "block"}, strike_options, {"display": "block"}

    return [], {"display": "none"}, [], {"display": "none"}


@app.callback(
    Output("weight-dropdown", "style"),
    Input("tenor-dropdown", "value"),
)
def show_weight_dropdown(tenor_value):
    if tenor_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    Output("tenor-dpdown-2", "style"),
    Input("weight-dropdown", "value"),
)
def show_tenor_dropdown_2(weight_value):
    if weight_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    Output("type-dpdown-2", "style"),
    Input("tenor-dpdown-2", "value"),
)
def show_type_dropdown_2(tenor_value):
    if tenor_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    [
        Output("min-strike-dropdown-2", "options"),
        Output("min-strike-dropdown-2", "style"),
        Output("max-strike-dropdown-2", "options"),
        Output("max-strike-dropdown-2", "style"),
    ],
    [
        Input("tenor-dpdown-2", "value"),
        Input("type-dpdown-2", "value"),
    ],
)
def update_strike_options_2(tenor_value, cp_value):
    tenors_1 = ["1w", "2w", "3w"]
    tenors_2 = ["1m", "2m", "3m"]
    tenors_3 = ["6m", "1y", "2y"]

    strike_options = []

    if cp_value and tenor_value:
        if cp_value == "Call":
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

        return strike_options, {"display": "block"}, strike_options, {"display": "block"}

    return [], {"display": "none"}, [], {"display": "none"}


@app.callback(
    Output("weight-dpdown-2", "style"),
    Input("tenor-dpdown-2", "value"),
)
def show_weight_dropdown_2(tenor_value):
    if tenor_value:
        return {"display": "block"}
    return {"display": "none"}


@app.callback(
    Output("graph-output", "figure"),
    Input("confirm-button-solver", "n_clicks"),
    State("ticker-dropdown", "value"),
    State("spot-forward-dropdown", "value"),
    State("type-dropdown", "value"),
    State("min-strike-dropdown", "value"),
    State("max-strike-dropdown", "value"),
    State("tenor-dropdown", "value"),
    State("weight-dropdown", "value"),
    State("type-dpdown-2", "value"),
    State("min-strike-dropdown-2", "value"),
    State("max-strike-dropdown-2", "value"),
    State("tenor-dpdown-2", "value"),
    State("weight-dpdown-2", "value"),
)
def update_matrix(
    n_clicks,
    asset,
    sf_value,
    type_1,
    min_strike_1,
    max_strike_1,
    tenor_1,
    weight_1,
    type_2,
    min_strike_2,
    max_strike_2,
    tenor_2,
    weight_2,
):
    if not n_clicks:
        raise PreventUpdate

    required_values = [
        asset,
        sf_value,
        type_1,
        min_strike_1,
        max_strike_1,
        tenor_1,
        weight_1,
        type_2,
        min_strike_2,
        max_strike_2,
        tenor_2,
        weight_2,
    ]
    if any(v is None for v in required_values):
        fig = go.Figure()
        fig.update_layout(title="Please fill all solver inputs before calculating.", height=500)
        return fig

    try:
        pivot_df, value_df, obs_df, label_df, metadata = build_solver_matrix(
            ticker=asset,
            sf_value=sf_value,
            type_1=type_1,
            tenor_1=tenor_1,
            min_strike_1=float(min_strike_1),
            max_strike_1=float(max_strike_1),
            weight_1=float(weight_1),
            type_2=type_2,
            tenor_2=tenor_2,
            min_strike_2=float(min_strike_2),
            max_strike_2=float(max_strike_2),
            weight_2=float(weight_2),
        )
    except Exception as exc:
        fig = go.Figure()
        fig.update_layout(title=f"Solver error: {exc}", height=500)
        return fig

    if pivot_df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data available for this combination.", height=500)
        return fig

    latest_date = metadata["latest_date"]
    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"

    hover_text = []
    for y in pivot_df.index:
        row = []
        for x in pivot_df.columns:
            pct = pivot_df.loc[y, x]
            val = value_df.loc[y, x] if (y in value_df.index and x in value_df.columns) else np.nan
            obs = obs_df.loc[y, x] if (y in obs_df.index and x in obs_df.columns) else np.nan
            label = label_df.loc[y, x] if (y in label_df.index and x in label_df.columns) else ""

            pct_str = f"{pct:.1f}" if pd.notna(pct) else "nan"
            val_str = f"{val:.4f}" if pd.notna(val) else "nan"
            obs_str = str(int(obs)) if pd.notna(obs) else "0"

            row.append(
                f"{label}<br>"
                f"Percentile: {pct_str}<br>"
                f"Combo value: {val_str}<br>"
                f"Obs: {obs_str}<br>"
                f"Latest date: {latest_date_str}"
            )
        hover_text.append(row)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale=list(
                reversed(
                    [
                        [0, "red"],
                        [0.5, "white"],
                        [1, "lightblue"],
                    ]
                )
            ),
            zmin=0,
            zmax=100,
            text=np.round(pivot_df.values, 1),
            texttemplate="%{text}",
            customdata=np.array(hover_text, dtype=object),
            hovertemplate="%{customdata}<extra></extra>",
            colorbar=dict(title="Percentile"),
        )
    )

    fig.update_layout(
        title=(
            f"{asset.split()[0]} {metadata['surface_type']} solver grid"
            f"<br><sup>Latest available date: {latest_date_str} | "
            f"Combos: {metadata['total_combos']}</sup>"
        ),
        xaxis_title=f"{tenor_1} {type_1} Strike",
        yaxis_title=f"{tenor_2} {type_2} Strike",
        xaxis=dict(
            tickmode="array",
            tickvals=list(pivot_df.columns),
            ticktext=[str(i) for i in pivot_df.columns],
            side="top",
        ),
        yaxis=dict(
            tickmode="array",
            tickvals=list(pivot_df.index),
            ticktext=[str(i) for i in pivot_df.index],
        ),
        autosize=False,
        width=1000,
        height=650,
    )

    return fig


@app.callback(
    Output("solver-detail-summary", "children"),
    Output("solver-detail-price-graph", "figure"),
    Output("solver-detail-percentile-graph", "figure"),
    Input("graph-output", "clickData"),
    State("ticker-dropdown", "value"),
    State("spot-forward-dropdown", "value"),
    State("type-dropdown", "value"),
    State("tenor-dropdown", "value"),
    State("weight-dropdown", "value"),
    State("type-dpdown-2", "value"),
    State("tenor-dpdown-2", "value"),
    State("weight-dpdown-2", "value"),
)
def update_solver_detail(
    click_data,
    asset,
    sf_value,
    type_1,
    tenor_1,
    weight_1,
    type_2,
    tenor_2,
    weight_2,
):
    if not click_data:
        empty_fig = go.Figure()
        empty_fig.update_layout(height=350, title="Click a heatmap cell to see combo history")
        return html.Div("Click a heatmap cell to see combo details."), empty_fig, empty_fig

    try:
        strike_1 = float(click_data["points"][0]["x"])
        strike_2 = float(click_data["points"][0]["y"])

        detail = get_combo_detail(
            ticker=asset,
            sf_value=sf_value,
            type_1=type_1,
            tenor_1=tenor_1,
            strike_1=strike_1,
            weight_1=float(weight_1),
            type_2=type_2,
            tenor_2=tenor_2,
            strike_2=strike_2,
            weight_2=float(weight_2),
        )
    except Exception as exc:
        empty_fig = go.Figure()
        empty_fig.update_layout(height=350, title=f"Detail error: {exc}")
        return html.Div(f"Detail error: {exc}"), empty_fig, empty_fig

    latest_date = detail["latest_date"]
    latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"
    latest_value = detail["latest_value"]
    latest_percentile = detail["latest_percentile"]
    obs_count = detail["observation_count"]

    summary = dbc.Card(
        dbc.CardBody(
            [
                html.H5(detail["combo_label"]),
                html.P(f"Surface: {detail['surface_type']}"),
                html.P(f"Latest date: {latest_date_str}"),
                html.P(f"Current combo value: {latest_value:.4f}" if pd.notna(latest_value) else "Current combo value: N/A"),
                html.P(f"Current percentile: {latest_percentile:.2f}" if pd.notna(latest_percentile) else "Current percentile: N/A"),
                html.P(f"Historical observations used: {obs_count}"),
            ]
        ),
        style={"marginBottom": "20px"},
    )

    price_df = detail["price_series"]
    percentile_df = detail["percentile_series"]

    if not price_df.empty:
        price_fig = go.Figure(
            data=[go.Scatter(x=price_df["date"], y=price_df["value"], mode="lines")]
        )
        price_fig.update_layout(
            title="Combo Value History",
            xaxis_title="Date",
            yaxis_title="Combo Value",
            height=350,
        )
    else:
        price_fig = go.Figure()
        price_fig.update_layout(title="No combo price history", height=350)

    if not percentile_df.empty:
        percentile_fig = go.Figure(
            data=[go.Scatter(x=percentile_df["date"], y=percentile_df["percentile"], mode="lines")]
        )
        percentile_fig.update_layout(
            title="Rolling 2Y Percentile History",
            xaxis_title="Date",
            yaxis_title="Percentile",
            height=350,
        )
    else:
        percentile_fig = go.Figure()
        percentile_fig.update_layout(title="No percentile history", height=350)

    return summary, price_fig, percentile_fig
