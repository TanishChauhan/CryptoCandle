"""Streamlit dashboard for crypto analytics and pipeline health."""

from __future__ import annotations

import time
from datetime import datetime, timezone

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

st.set_page_config(page_title="Crypto ETL Dashboard", layout="wide", page_icon="📈")

SYMBOLS = get_symbols()


def _auto_refresh(seconds: int = 30) -> None:
    now = time.time()
    if "last_auto_refresh" not in st.session_state:
        st.session_state.last_auto_refresh = now
        return
    if now - st.session_state.last_auto_refresh >= seconds:
        st.session_state.last_auto_refresh = now
        st.rerun()


def render_analytics_tab(symbol: str) -> None:
    st.subheader(f"{symbol} — 1-minute market view")
    candles = fetch_candles(symbol)

    if candles.empty:
        st.info("No candle data yet. Start producer + spark-streaming to populate analytics.candles_1m.")
        return

    latest = candles["window_start"].max()
    st.caption(
        f"{len(candles)} candles loaded · latest window {latest} · "
        f"refreshed {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC (every 30s)"
    )

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
        height=650,
        xaxis_rangeslider_visible=False,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
        margin={"l": 20, "r": 20, "t": 50, "b": 20},
    )
    st.plotly_chart(fig, use_container_width=True)

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
        vol_fig.update_layout(height=280, margin={"l": 20, "r": 20, "t": 20, "b": 20})
        st.plotly_chart(vol_fig, use_container_width=True)

    with right:
        st.markdown("**Latest daily summary**")
        daily = fetch_daily_summary(symbol)
        if daily.empty:
            st.caption("Daily summary will appear after the Airflow daily batch job runs.")
        else:
            st.dataframe(daily.tail(7), use_container_width=True, hide_index=True)

    st.markdown("**Recent 1m candles**")
    st.dataframe(candles.tail(20), use_container_width=True, hide_index=True)


def render_health_tab() -> None:
    st.subheader("Pipeline health")

    summary = fetch_dlq_summary(hours=24)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Records in (24h)", f"{summary['records_in']:,}")
    c2.metric("DLQ records (24h)", f"{summary['records_dlq']:,}")
    c3.metric("DLQ ratio (24h)", f"{summary['dlq_ratio']:.2%}")
    c4.metric("Quality status", summary["status"].upper())

    if summary["status"] == "warn":
        st.warning(summary["message"])
    elif summary["status"] == "fail":
        st.error(summary["message"])
    else:
        st.success(summary["message"])

    metrics = fetch_pipeline_metrics(limit=100)
    if metrics.empty:
        st.info("No pipeline metrics yet.")
        return

    latest = metrics.iloc[0]
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Latest job", str(latest["job_name"]))
    m2.metric("Latest batch ms", int(latest["batch_duration_ms"] or 0))
    m3.metric("Latest records in", int(latest["records_in"] or 0))
    m4.metric("Latest kafka lag", int(latest["kafka_lag"] or 0))

    st.markdown("**Throughput over time**")
    throughput = metrics.sort_values("recorded_at")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=throughput["recorded_at"],
            y=throughput["records_in"],
            mode="lines+markers",
            name="records_in",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=throughput["recorded_at"],
            y=throughput["records_dlq"],
            mode="lines+markers",
            name="records_dlq",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=throughput["recorded_at"],
            y=throughput["kafka_lag"],
            mode="lines+markers",
            name="kafka_lag",
        ),
        secondary_y=True,
    )
    fig.update_layout(height=360, margin={"l": 20, "r": 20, "t": 30, "b": 20})
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Recent pipeline runs**")
    st.dataframe(metrics.head(25), use_container_width=True, hide_index=True)


def main() -> None:
    st.title("Crypto Analytics Platform")
    st.caption("Live Binance data → Kafka → Spark → PostgreSQL")

    analytics_tab, health_tab = st.tabs(["Analytics", "Pipeline Health"])

    with analytics_tab:
        symbol = st.selectbox("Symbol", SYMBOLS, index=0)
        st.caption("Charts refresh automatically every 30 seconds. Spark may add a new 1m candle every 2–3 minutes under load.")
        render_analytics_tab(symbol)

    with health_tab:
        render_health_tab()

    _auto_refresh(30)


if __name__ == "__main__":
    main()
