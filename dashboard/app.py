"""
Banking Data Reliability Platform — Pipeline Health & Business KPIs Dashboard

Run locally:
    streamlit run dashboard/app.py
"""
import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Banking Data Platform", page_icon="🏦", layout="wide")

# ------------------------------------------------------------------
# DB connection
# ------------------------------------------------------------------
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_USER = os.environ.get("DB_USER", "airflow")
DB_PASS = os.environ.get("DB_PASS", "airflow")
DB_NAME = os.environ.get("DB_NAME", "airflow")


@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME
    )


def query(sql: str) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


# ------------------------------------------------------------------
# Layout
# ------------------------------------------------------------------
st.title("🏦 Banking Data Reliability Platform")
st.caption("Pipeline health + business KPIs · Powered by Airflow, dbt, PostgreSQL")

# ------------------------------------------------------------------
# Top-level KPIs — last 30 days of business data
# ------------------------------------------------------------------
try:
    kpi = query("""
        SELECT
            (SELECT COUNT(*) FROM analytics.fact_transactions) AS total_transactions,
            (SELECT COALESCE(SUM(amount), 0) FROM analytics.fact_transactions) AS total_value,
            (SELECT COUNT(*) FROM analytics.dim_customer) AS customers,
            (SELECT COUNT(*) FROM analytics.dim_account) AS accounts,
            (SELECT COUNT(*) FROM quarantine.transactions) AS quarantined;
    """).iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total transactions", f"{int(kpi['total_transactions']):,}")
    col2.metric("Total value", f"€{kpi['total_value']:,.0f}")
    col3.metric("Customers", f"{int(kpi['customers']):,}")
    col4.metric("Accounts", f"{int(kpi['accounts']):,}")
    col5.metric("Quarantined", f"{int(kpi['quarantined']):,}")
except Exception as e:
    st.error(f"Could not load KPIs: {e}")

st.divider()

# ------------------------------------------------------------------
# Pipeline health — last 10 runs
# ------------------------------------------------------------------
st.subheader("📊 Pipeline health (last 10 runs)")

try:
    runs = query("""
        SELECT
            run_id,
            dag_run_id,
            started_at,
            finished_at,
            status,
            records_extracted,
            records_quarantined,
            ROUND(duration_seconds::numeric, 1) AS duration_seconds
        FROM ops.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 10;
    """)
    if not runs.empty:
        runs["started_at"] = pd.to_datetime(runs["started_at"]).dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(runs, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline runs recorded yet. Run the DAG once to populate this view.")
except Exception as e:
    st.warning(f"Could not load pipeline runs: {e}")

# ------------------------------------------------------------------
# Business trend — daily transaction value
# ------------------------------------------------------------------
st.subheader("📈 Daily transaction value")

try:
    trend = query("""
        SELECT metric_date, total_transactions, total_value, success_rate
        FROM analytics.daily_transaction_metrics
        ORDER BY metric_date;
    """)
    if not trend.empty:
        fig = px.line(
            trend, x="metric_date", y="total_value",
            title="Daily transaction value (€)",
            labels={"total_value": "Value (€)", "metric_date": "Date"},
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No metric data yet.")
except Exception as e:
    st.warning(f"Could not load trends: {e}")

# ------------------------------------------------------------------
# Quarantine breakdown
# ------------------------------------------------------------------
st.subheader("⚠️ Quarantined records by reason")

try:
    q = query("""
        SELECT reject_reason, COUNT(*) AS count
        FROM quarantine.transactions
        GROUP BY reject_reason
        ORDER BY count DESC;
    """)
    if not q.empty:
        fig = px.bar(
            q, x="reject_reason", y="count", color="reject_reason",
            title="Records rejected during validation",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No quarantined records.")
except Exception as e:
    st.warning(f"Could not load quarantine: {e}")

# ------------------------------------------------------------------
# Reconciliation
# ------------------------------------------------------------------
st.subheader("🔍 Latest reconciliation")

try:
    rec = query("""
        SELECT DISTINCT ON (metric_name)
            metric_name, source_value, target_value, difference,
            variance_pct, status, checked_at
        FROM ops.reconciliation
        ORDER BY metric_name, checked_at DESC;
    """)
    if not rec.empty:
        st.dataframe(rec, use_container_width=True, hide_index=True)
    else:
        st.info("No reconciliation data yet.")
except Exception as e:
    st.warning(f"Could not load reconciliation: {e}")

st.divider()
st.caption(f"Last refreshed: {datetime.utcnow().isoformat()}Z")
