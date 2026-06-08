"""Streamlit dashboard for crypto analytics and pipeline health."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.db import (
    fetch_candles,
    fetch_daily_summary,
    fetch_dlq_summary,
    fetch_pipeline_metrics,
    get_symbols,
)
from dashboard.labels import friendly_job_name

st.set_page_config(page_title="Crypto ETL Dashboard", layout="wide", page_icon="📈")

SYMBOLS = get_symbols()
REFRESH_SECONDS = max(10, int(os.getenv("DASHBOARD_REFRESH_SECONDS", "30")))
REFRESH_EVERY = timedelta(seconds=REFRESH_SECONDS)

# Plotly toolbar: zoom, pan, reset + mouse-wheel zoom on chart.
PLOTLY_CHART_CONFIG: dict = {
    "scrollZoom": True,
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
}

_TIME_RANGE_BUTTONS = [
    dict(count=15, label="15m", step="minute", stepmode="backward"),
    dict(count=1, label="1h", step="hour", stepmode="backward"),
    dict(count=4, label="4h", step="hour", stepmode="backward"),
    dict(step="all", label="All"),
]


def _time_axis_options(*, include_selector: bool = True) -> dict:
    opts: dict = {
        "type": "date",
        "showspikes": True,
        "spikemode": "across",
        "spikesnap": "cursor",
        "rangeslider": {"visible": True, "thickness": 0.06},
    }
    if include_selector:
        opts["rangeselector"] = dict(
            buttons=_TIME_RANGE_BUTTONS,
            x=0,
            y=1.18,
            xanchor="left",
            yanchor="top",
        )
    return opts


def _apply_time_navigation(
    fig: go.Figure,
    *,
    use_subplots: bool = False,
    slider_row: int = 1,
    selector_row: int | None = None,
    include_selector: bool = True,
) -> None:
    """Time buttons + bottom rangeslider. Subplot figures must pass ``use_subplots=True``."""
    selector_row = selector_row if selector_row is not None else slider_row
    selector_opts = _time_axis_options(include_selector=include_selector)
    slider_opts = {"rangeslider": selector_opts.pop("rangeslider")}

    if use_subplots:
        fig.update_xaxes(**selector_opts, row=selector_row, col=1)
        fig.update_xaxes(**slider_opts, row=slider_row, col=1)
    else:
        fig.update_xaxes(**selector_opts, **slider_opts)


def render_analytics_tab(symbol: str) -> None:
    st.subheader(f"{symbol} — 1-minute market view")
    candles = fetch_candles(symbol)

    if candles.empty:
        st.info("No candle data yet. Start producer + spark-streaming to populate analytics.candles_1m.")
        return

    latest = candles["window_start"].max()
    earliest = candles["window_start"].min()
    st.caption(
        f"{len(candles)} candles loaded · range {earliest} → {latest} · "
        f"refreshed {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC (every {REFRESH_SECONDS}s) · "
        "Tip: use the range buttons (15m/1h/4h/All), drag the bottom slider, scroll to zoom, drag to pan, "
        "double-click to reset."
    )

    uirevision = symbol

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],
        subplot_titles=("Price (1m OHLC)", "Volume"),
    )
    fig.add_trace(
        go.Candlestick(
            x=candles["window_start"],
            open=candles["open_price"],
            high=candles["high_price"],
            low=candles["low_price"],
            close=candles["close_price"],
            name="OHLC",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=candles["window_start"],
            y=candles["avg_price"],
            mode="lines",
            name="Avg Price",
            line={"color": "#636EFA", "width": 1.5},
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=candles["window_start"],
            y=candles["volume"],
            name="Volume",
            marker_color="#00CC96",
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        height=720,
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "top", "y": 1.16, "x": 1, "xanchor": "right"},
        margin={"l": 20, "r": 20, "t": 70, "b": 40},
        uirevision=uirevision,
    )
    _apply_time_navigation(fig, use_subplots=True, slider_row=2, selector_row=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    st.plotly_chart(
        fig,
        use_container_width=True,
        key=f"ohlc-{symbol}",
        config=PLOTLY_CHART_CONFIG,
    )

    left, right = st.columns(2)
    with left:
        st.markdown("**Rolling volatility (1m stddev proxy)**")
        vol_fig = go.Figure(
            data=[
                go.Scatter(
                    x=candles["window_start"],
                    y=candles["volatility"],
                    mode="lines+markers",
                    name="Volatility",
                    line={"color": "#EF553B"},
                )
            ]
        )
        vol_fig.update_layout(
            height=320,
            hovermode="x unified",
            margin={"l": 20, "r": 20, "t": 40, "b": 40},
            uirevision=uirevision,
            yaxis={"title": "Volatility"},
        )
        _apply_time_navigation(vol_fig)
        st.plotly_chart(
            vol_fig,
            use_container_width=True,
            key=f"vol-{symbol}",
            config=PLOTLY_CHART_CONFIG,
        )

    with right:
        st.markdown("**Latest daily summary**")
        daily = fetch_daily_summary(symbol)
        if daily.empty:
            st.caption("Daily summary will appear after the Airflow daily batch job runs.")
        else:
            st.dataframe(daily.tail(7), use_container_width=True, hide_index=True)

    st.markdown("**Recent 1m candles**")
    st.dataframe(candles.tail(20), use_container_width=True, hide_index=True)


def _format_duration_ms(ms: int | float | None) -> str:
    if ms is None:
        return "—"
    seconds = float(ms) / 1000.0
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


def render_health_tab(*, chart_hours: int) -> None:
    st.subheader("Pipeline health")
    st.caption(
        f"Summary covers the last 24 hours · chart shows the last {chart_hours} hour(s) · "
        f"refreshes every {REFRESH_SECONDS}s"
    )

    summary = fetch_dlq_summary(hours=24)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Trades processed (24h)", f"{summary['records_in']:,}")
    c2.metric("Rejected records (24h)", f"{summary['records_dlq']:,}")
    c3.metric("Error rate (24h)", f"{summary['dlq_ratio']:.2%}")
    status_label = {"ok": "Healthy", "warn": "Warning", "fail": "Critical"}.get(summary["status"], summary["status"])
    c4.metric("Overall status", status_label)

    if summary["status"] == "warn":
        st.warning(summary["message"])
    elif summary["status"] == "fail":
        st.error(summary["message"])

    metrics = fetch_pipeline_metrics()
    if metrics.empty:
        st.info("No pipeline activity recorded yet. Start producer and Spark streaming.")
        return

    metrics = metrics.copy()
    metrics["recorded_at"] = pd.to_datetime(metrics["recorded_at"], utc=True)
    metrics["job_label"] = metrics["job_name"].apply(friendly_job_name)

    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=chart_hours)
    chart_df = metrics.loc[metrics["recorded_at"] >= cutoff].sort_values("recorded_at")
    if chart_df.empty:
        st.info(f"No activity in the last {chart_hours} hour(s). Try a wider chart window.")
        chart_df = metrics.sort_values("recorded_at").tail(50)

    st.markdown("#### Throughput")
    fig = go.Figure()
    for job_name, group in chart_df.groupby("job_name", sort=False):
        label = friendly_job_name(job_name)
        fig.add_trace(
            go.Scatter(
                x=group["recorded_at"],
                y=group["records_in"],
                mode="lines",
                name=label,
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "Time: %{x}<br>"
                    "Records: %{y:,}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        height=400,
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "top", "y": 1.0, "x": 1, "xanchor": "right"},
        margin={"l": 20, "r": 20, "t": 24, "b": 40},
        yaxis={"title": "Records per batch", "tickformat": ",d"},
        uirevision="pipeline-health",
    )
    # Chart window is controlled by the selectbox above; keep only the bottom scrub bar here.
    _apply_time_navigation(fig, include_selector=False)
    st.plotly_chart(fig, use_container_width=True, key="health-throughput", config=PLOTLY_CHART_CONFIG)

    st.markdown("#### Latest Spark batches")
    spark_jobs = {"spark_valid_stream", "spark_raw_valid_trades", "spark_invalid_stream"}
    latest_by_job = (
        metrics.loc[metrics["job_name"].isin(spark_jobs)]
        .sort_values("recorded_at", ascending=False)
        .groupby("job_name", as_index=False)
        .first()
    )
    if latest_by_job.empty:
        st.caption("No Spark streaming metrics yet.")
    else:
        display_rows = []
        for _, row in latest_by_job.iterrows():
            display_rows.append(
                {
                    "Pipeline step": friendly_job_name(row["job_name"]),
                    "Last run (UTC)": row["recorded_at"].strftime("%Y-%m-%d %H:%M:%S"),
                    "Batch duration": _format_duration_ms(row["batch_duration_ms"]),
                    "Records": int(row["records_in"] or 0),
                    "Rejected": int(row["records_dlq"] or 0),
                }
            )
        st.dataframe(pd.DataFrame(display_rows), use_container_width=True, hide_index=True)

    with st.expander("Technical log (raw metrics)"):
        log_view = metrics.head(40).copy()
        log_view["recorded_at"] = log_view["recorded_at"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        log_view["batch_duration_ms"] = log_view["batch_duration_ms"].apply(
            lambda v: _format_duration_ms(v) if pd.notna(v) else "—"
        )
        log_view = log_view.rename(
            columns={
                "recorded_at": "Time (UTC)",
                "job_label": "Step",
                "records_in": "Records in",
                "records_dlq": "Rejected",
                "batch_duration_ms": "Duration",
                "status": "Status",
            }
        )
        st.dataframe(
            log_view[["Time (UTC)", "Step", "Records in", "Rejected", "Duration", "Status"]],
            use_container_width=True,
            hide_index=True,
        )


def main() -> None:
    st.title("Crypto Analytics Platform")
    st.caption("Live Binance data → Kafka → Spark → PostgreSQL")

    analytics_tab, health_tab = st.tabs(["Analytics", "Pipeline Health"])

    with analytics_tab:
        symbol = st.selectbox("Symbol", SYMBOLS, index=0)
        st.caption(
            f"Charts auto-refresh every {REFRESH_SECONDS} seconds. "
            "New 1m candles appear after Spark closes each window."
        )

        @st.fragment(run_every=REFRESH_EVERY)
        def live_analytics() -> None:
            render_analytics_tab(symbol)

        live_analytics()

    with health_tab:
        chart_hours = st.selectbox(
            "Chart time window",
            options=[1, 4, 12, 24],
            index=1,
            format_func=lambda h: f"Last {h} hour{'s' if h != 1 else ''}",
        )

        @st.fragment(run_every=REFRESH_EVERY)
        def live_health() -> None:
            render_health_tab(chart_hours=chart_hours)

        live_health()


if __name__ == "__main__":
    main()
