# dashboard.py
import os
import sys
from datetime import datetime, date
from typing import Optional, Tuple

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import awswrangler as wr

# =========================
# App Config & Constants
# =========================
DB = "calendly_marketing"

TBL_BOOKINGS_BY_SOURCE_DAY = "gold_bookings_by_source_day"
TBL_CPB_BY_CHANNEL         = "gold_cost_per_booking_by_channel"
TBL_BOOKINGS_TREND         = "gold_bookings_trend_over_time"
TBL_ATTRIBUTION            = "gold_channel_attribution_leaderboard"
TBL_TIMESLOT               = "gold_bookings_by_timeslot"
TBL_MEETINGS_WEEK          = "gold_meetings_per_employee_week"

st.set_page_config(
    page_title="Calendly Marketing Performance Dashboard",
    page_icon="📊",
    layout="wide"
)

# =========================
# Athena helpers
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def _run_athena(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    try:
        df = wr.athena.read_sql_query(sql=sql, database=DB, params=params or {})
        df.columns = [c.strip().lower() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Athena query failed: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=300)
def _get_dt_bounds() -> Tuple[Optional[str], Optional[str]]:
    sql = f"SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt FROM {DB}.{TBL_BOOKINGS_BY_SOURCE_DAY}"
    df = _run_athena(sql)
    if df.empty or df["min_dt"].isna().all():
        return None, None
    return df.iloc[0]["min_dt"], df.iloc[0]["max_dt"]

def _dt_where_clause(dt_from: Optional[str], dt_to: Optional[str]) -> str:
    clause = []
    if dt_from:
        clause.append(f"dt >= '{dt_from}'")
    if dt_to:
        clause.append(f"dt <= '{dt_to}'")
    return ("WHERE " + " AND ".join(clause)) if clause else ""

# =========================
# Header & Global Filters
# =========================
st.markdown("## Calendly Marketing Performance Dashboard")
st.caption("Live data from Gold Layer via Athena (uses your AWS profile).")

min_dt, max_dt = _get_dt_bounds()
default_start = date.fromisoformat(max_dt) if max_dt else date.today()
default_end   = date.fromisoformat(max_dt) if max_dt else date.today()

with st.expander("Global Filters", expanded=True):
    cols = st.columns([1, 1, 2, 2])
    with cols[0]:
        dt_start = st.date_input(
            "Start (dt partition)",
            value=default_start,
            min_value=date.fromisoformat(min_dt) if min_dt else date.today(),
            max_value=date.fromisoformat(max_dt) if max_dt else date.today(),
            format="YYYY-MM-DD",
            key="dt_start",
        ) if min_dt else None
    with cols[1]:
        dt_end = st.date_input(
            "End (dt partition)",
            value=default_end,
            min_value=date.fromisoformat(min_dt) if min_dt else date.today(),
            max_value=date.fromisoformat(max_dt) if max_dt else date.today(),
            format="YYYY-MM-DD",
            key="dt_end",
        ) if max_dt else None

    if dt_start and dt_end and dt_start > dt_end:
        st.warning("Start date is after End date—swapping for you.")
        dt_start, dt_end = dt_end, dt_start

    st.caption(
        "Defaults to the latest available partition (`max(dt)`); adjust to view previous days."
    )

dt_from_str = dt_start.isoformat() if isinstance(dt_start, date) else None
dt_to_str   = dt_end.isoformat() if isinstance(dt_end, date) else None
WHERE_DT = _dt_where_clause(dt_from_str, dt_to_str)

st.divider()

# =========================
# Tabs
# =========================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "1.1 Daily Calls by Source",
    "1.2 Cost per Booking (CPB) by Channel",
    "1.3 Bookings Trend Over Time",
    "1.4 Channel Attribution",
    "1.5 Volume by Time Slot / DOW",
    "1.6 Meetings per Employee (Weekly)",
])

# -------------------------------------------------
# 1.1 Daily Calls Booked by Source
# -------------------------------------------------
with tab1:
    st.subheader("Daily Calls Booked by Source")

    sql = f"""
        SELECT booking_date, source, COUNT(DISTINCT booking_id) AS bookings
        FROM {DB}.{TBL_BOOKINGS_BY_SOURCE_DAY}
        {WHERE_DT}
        GROUP BY booking_date, source
        ORDER BY booking_date
    """
    df = _run_athena(sql)
    if df.empty:
        st.info("No data for the selected period.")
    else:
        total_calls = int(df["bookings"].sum())
        active_days = df["booking_date"].nunique()
        active_sources = df["source"].nunique()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Bookings", f"{total_calls:,}")
        c2.metric("Active Days", f"{active_days:,}")
        c3.metric("Sources", f"{active_sources:,}")

        fig = px.line(df, x="booking_date", y="bookings", color="source",
                      markers=True, title="Daily Bookings by Source")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=420)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Daily comparison table", expanded=False):
            st.dataframe(df.sort_values(["booking_date", "source"]), use_container_width=True)

# -------------------------------------------------
# 1.2 Cost Per Booking (CPB) by Channel
# -------------------------------------------------
with tab2:
    st.subheader("Cost per Booking (CPB) by Channel")

    sql = f"""
        SELECT
            channel,
            SUM(total_spend)    AS total_spend,
            SUM(total_bookings) AS total_bookings,
            CASE WHEN SUM(total_bookings) > 0
                 THEN SUM(total_spend) / SUM(total_bookings)
                 ELSE NULL END AS cpb
        FROM {DB}.{TBL_CPB_BY_CHANNEL}
        {WHERE_DT}
        GROUP BY channel
        ORDER BY cpb NULLS LAST, total_bookings DESC
    """
    df = _run_athena(sql)
    if df.empty:
        st.info("No CPB data for the selected period.")
    else:
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Bookings", f"{int(df['total_bookings'].sum()):,}")
        k2.metric("Total Spend", f"${df['total_spend'].sum():,.2f}")
        overall_cpb = (df["total_spend"].sum() / df["total_bookings"].sum()) if df["total_bookings"].sum() > 0 else np.nan
        k3.metric("Average CPB", f"${overall_cpb:,.2f}" if np.isfinite(overall_cpb) else "—")

        fig = px.bar(df.sort_values("cpb", na_position="last"),
                     x="channel", y="cpb", text_auto=".2f", title="CPB by Channel")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=420)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Channel Spend/Bookings/CPB", expanded=False):
            display = df.copy()
            display["total_spend"] = display["total_spend"].map(lambda v: f"${v:,.2f}")
            display["cpb"] = display["cpb"].map(lambda v: f"${v:,.2f}" if pd.notna(v) else "—")
            st.dataframe(display, use_container_width=True)

# -------------------------------------------------
# 1.3 Bookings Trend Over Time
# -------------------------------------------------
with tab3:
    st.subheader("Bookings Trend Over Time")

    sql = f"""
        WITH daily AS (
            SELECT booking_date, source, COUNT(DISTINCT booking_id) AS bookings
            FROM {DB}.{TBL_BOOKINGS_TREND}
            {WHERE_DT}
            GROUP BY booking_date, source
        )
        SELECT
            booking_date, source, bookings,
            SUM(bookings) OVER (PARTITION BY source ORDER BY booking_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_bookings
        FROM daily
        ORDER BY booking_date
    """
    df = _run_athena(sql)
    if df.empty:
        st.info("No trend data for the selected period.")
    else:
        fig1 = px.line(df, x="booking_date", y="bookings", color="source", markers=True,
                       title="Daily Bookings by Source")
        fig1.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380)
        st.plotly_chart(fig1, use_container_width=True)

        fig2 = px.area(df.sort_values("booking_date"), x="booking_date",
                       y="cumulative_bookings", color="source",
                       title="Cumulative Bookings (by Source)")
        fig2.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380)
        st.plotly_chart(fig2, use_container_width=True)

# -------------------------------------------------
# 1.4 Channel Attribution
# -------------------------------------------------
with tab4:
    st.subheader("Channel Attribution (Volume & CPB)")

    sql_leader = f"""
        SELECT
            source,
            SUM(total_bookings) AS total_bookings,
            SUM(spend)          AS total_spend,
            CASE WHEN SUM(total_bookings) > 0
                 THEN SUM(spend) / SUM(total_bookings)
                 ELSE NULL END AS cpb
        FROM {DB}.{TBL_ATTRIBUTION}
        {WHERE_DT}
        GROUP BY source
        ORDER BY total_bookings DESC, cpb ASC NULLS LAST
    """
    df_src = _run_athena(sql_leader)

    sql_heat = f"""
        SELECT source, campaign,
               SUM(total_bookings) AS total_bookings,
               SUM(spend)          AS total_spend,
               CASE WHEN SUM(total_bookings) > 0
                    THEN SUM(spend) / SUM(total_bookings)
                    ELSE NULL END AS cpb
        FROM {DB}.{TBL_ATTRIBUTION}
        {WHERE_DT}
        GROUP BY source, campaign
    """
    df_heat = _run_athena(sql_heat)

    if df_src.empty and df_heat.empty:
        st.info("No attribution data for the selected period.")
    else:
        if not df_src.empty:
            c1, c2 = st.columns([2, 3])
            with c1:
                st.markdown("**Leaderboard (Source)**")
                disp = df_src.copy()
                disp["total_spend"] = disp["total_spend"].map(lambda v: f"${v:,.2f}")
                disp["cpb"] = disp["cpb"].map(lambda v: f"${v:,.2f}" if pd.notna(v) else "—")
                st.dataframe(disp, use_container_width=True, height=380)

            with c2:
                top = df_src.sort_values(["total_bookings", "cpb"], ascending=[False, True]).head(10)
                fig = px.bar(top, x="source", y="total_bookings", text_auto=True, title="Top Sources by Bookings")
                fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380)
                st.plotly_chart(fig, use_container_width=True)

        if not df_heat.empty:
            st.markdown("**CPB Heatmap (Source × Campaign)**")
            pivot = df_heat.pivot_table(index="source", columns="campaign", values="cpb", aggfunc="mean")
            fig = px.imshow(pivot, labels=dict(x="Campaign", y="Source", color="CPB ($)"),
                            aspect="auto", title="CPB by Source & Campaign")
            fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=520)
            st.plotly_chart(fig, use_container_width=True)

# -------------------------------------------------
# 1.5 Booking Volume by Time Slot / Day of Week
# -------------------------------------------------
with tab5:
    st.subheader("Booking Volume by Time Slot / Day of Week")

    sql = f"""
        SELECT hour_of_day, day_of_week, source,
               COUNT(DISTINCT booking_id) AS bookings
        FROM {DB}.{TBL_TIMESLOT}
        {WHERE_DT}
        GROUP BY hour_of_day, day_of_week, source
    """
    df = _run_athena(sql)
    if df.empty:
        st.info("No timeslot data for the selected period.")
    else:
        heat = df.groupby(["day_of_week", "hour_of_day"], as_index=False)["bookings"].sum()
        day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        if set(heat["day_of_week"]).issubset(set(day_order)):
            heat["day_of_week"] = pd.Categorical(heat["day_of_week"], categories=day_order, ordered=True)
            heat = heat.sort_values(["day_of_week", "hour_of_day"])

        fig = px.density_heatmap(heat, x="hour_of_day", y="day_of_week", z="bookings",
                                 histfunc="avg", title="Heatmap: Hour of Day vs Day of Week (Bookings)")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=420)
        st.plotly_chart(fig, use_container_width=True)

        by_hour = df.groupby("hour_of_day", as_index=False)["bookings"].sum()
        fig2 = px.bar(by_hour, x="hour_of_day", y="bookings", text_auto=True, title="Histogram: Bookings by Hour")
        fig2.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=360)
        st.plotly_chart(fig2, use_container_width=True)

        by_day = df.groupby("day_of_week", as_index=False)["bookings"].sum()
        fig3 = px.pie(by_day, names="day_of_week", values="bookings",
                      title="Share of Bookings by Day of Week", hole=0.35)
        st.plotly_chart(fig3, use_container_width=True)

# -------------------------------------------------
# 1.6 Meetings per Employee (Weekly)
# -------------------------------------------------
with tab6:
    st.subheader("Meetings per Employee (Weekly)")

    sql = f"""
        SELECT host_user_name,
               SUM(meetings) AS total_meetings,
               AVG(CAST(avg_meetings_per_week AS DOUBLE)) AS avg_meetings
        FROM {DB}.{TBL_MEETINGS_WEEK}
        {WHERE_DT}
        GROUP BY host_user_name
        ORDER BY total_meetings DESC
    """
    df = _run_athena(sql)
    if df.empty:
        st.info("No meeting load data for the selected period.")
    else:
        total_meetings = int(df["total_meetings"].sum())
        max_emp = int(df["total_meetings"].max()) if not df["total_meetings"].isna().all() else 0
        min_emp = int(df["total_meetings"].min()) if not df["total_meetings"].isna().all() else 0
        mean_week = df["avg_meetings"].mean() if not df["avg_meetings"].isna().all() else np.nan

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Meetings", f"{total_meetings:,}")
        k2.metric("Max Meetings (Emp)", f"{max_emp:,}")
        k3.metric("Min Meetings (Emp)", f"{min_emp:,}")
        k4.metric("Avg Meetings/Week (Mean)", f"{mean_week:,.2f}" if pd.notna(mean_week) else "—")

        fig = px.bar(df.sort_values("avg_meetings", ascending=False),
                     x="host_user_name", y="avg_meetings",
                     text_auto=".2f", title="Average Meetings per Week by Employee")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=420)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Employee Meeting Stats", expanded=False):
            st.dataframe(df, use_container_width=True)

# =========================
# Footer
# =========================
st.caption("Data sourced from Gold Layer (partitioned by `dt`, appended daily).")
