"""
Banking Data Reliability Platform — dual-source dashboard.

Reads committed CSV snapshots (no database connection required).
Works locally and on Streamlit Cloud.
"""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Banking Data Platform",
    page_icon="🏦",
    layout="wide",
)

# ------------------------------------------------------------------
# Paths — relative to repo root (works on Streamlit Cloud)
# ------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
SYNTHETIC_DIR = ROOT / "data" / "synthetic"
FDIC_DIR      = ROOT / "data" / "fdic"


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.warning(f"Could not read {path.name}: {e}")
        return pd.DataFrame()


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


# ------------------------------------------------------------------
# Sidebar — data source toggle
# ------------------------------------------------------------------
st.sidebar.title("🏦 Data Source")
source = st.sidebar.radio(
    "Choose a pipeline",
    options=["Synthetic (Faker)", "Real (FDIC BankFind)"],
    index=0,
)

st.sidebar.divider()
st.sidebar.caption(
    "Both pipelines run daily via GitHub Actions. "
    "The dashboard reads committed CSV snapshots — no live database."
)

# ------------------------------------------------------------------
# Render the selected source
# ------------------------------------------------------------------
if source == "Synthetic (Faker)":
    st.title("🏦 Banking Data Reliability Platform — Synthetic Pipeline")
    st.caption("Faker-generated transactions with intentionally injected errors.")

    fact     = load_csv(SYNTHETIC_DIR / "fact_transactions.csv")
    metrics  = load_csv(SYNTHETIC_DIR / "daily_metrics.csv")
    quarantine = load_csv(SYNTHETIC_DIR / "quarantine.csv")
    runs     = load_csv(SYNTHETIC_DIR / "pipeline_runs.csv")

    if fact.empty:
        st.info("No synthetic data yet. Run the pipeline once.")
        st.stop()

    # KPI cards
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Transactions", f"{len(fact):,}")
    c2.metric("Total value (€)", f"{fact['amount'].sum():,.0f}")
    c3.metric("Countries", fact['customer_country'].nunique())
    c4.metric("Merchants", fact['merchant'].nunique())
    c5.metric("Quarantined", int(quarantine['count'].sum()) if not quarantine.empty else 0)

    st.divider()

    # Daily value trend
    if not metrics.empty:
        metrics["metric_date"] = pd.to_datetime(metrics["metric_date"])
        fig = px.line(metrics.sort_values("metric_date"),
                      x="metric_date", y="total_value",
                      title="Daily transaction value (€)")
        st.plotly_chart(fig, use_container_width=True)

    # Transaction status breakdown
    status_counts = fact["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    fig = px.bar(status_counts, x="status", y="count",
                 color="status", title="Transactions by status")
    st.plotly_chart(fig, use_container_width=True)

    # Quarantine
    if not quarantine.empty:
        fig = px.bar(quarantine, x="reject_reason", y="count",
                     color="reject_reason", title="Quarantined rows by reason")
        st.plotly_chart(fig, use_container_width=True)

    # Pipeline health
    if not runs.empty:
        st.subheader("Pipeline health (recent runs)")
        st.dataframe(runs.head(10), use_container_width=True, hide_index=True)


else:  # FDIC
    st.title("🏦 Banking Data Reliability Platform — FDIC Pipeline")
    st.caption("Live data from the FDIC BankFind Suite API.")

    institutions = load_csv(FDIC_DIR / "institutions.csv")
    meta         = load_json(FDIC_DIR / "meta.json")

    if institutions.empty:
        st.info("No FDIC data yet. Run the pipeline once.")
        st.stop()

    institutions["ASSET"] = pd.to_numeric(institutions["ASSET"], errors="coerce")

    # KPI cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Institutions", f"{len(institutions):,}")
    c2.metric("States", institutions["STALP"].nunique())
    c3.metric("Total assets", f"${institutions['ASSET'].sum()/1e6:,.0f}B")
    c4.metric("Median assets", f"${institutions['ASSET'].median()/1e6:,.1f}M")

    if meta:
        st.caption(f"Last generated: {meta.get('generated_at', 'unknown')}")

    st.divider()

    # Top 20 banks by assets
    top = institutions.nlargest(20, "ASSET")[["NAME", "STALP", "ASSET"]].copy()
    top["ASSET"] = top["ASSET"] / 1e6  # to billions
    fig = px.bar(top, x="ASSET", y="NAME", orientation="h",
                 title="Top 20 banks by total assets (billions USD)",
                 labels={"ASSET": "Assets ($B)", "NAME": ""})
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    # Assets by state
    by_state = (institutions.groupby("STALP", as_index=False)
                .agg(banks=("CERT", "count"), assets=("ASSET", "sum")))
    by_state["assets_b"] = by_state["assets"] / 1e6
    fig = px.choropleth(
        by_state, locations="STALP", locationmode="USA-states",
        color="assets_b", scope="usa",
        color_continuous_scale="Blues",
        title="Total bank assets by state (billions USD)",
    )
    st.plotly_chart(fig, use_container_width=True)

    #st.subheader("All institutions")
    #st.dataframe(institutions, use_container_width=True, hide_index=True)
    
    st.subheader("All institutions")
    # Show only the columns that matter, in a sensible order
    display_cols = [c for c in [
        "CERT", "NAME", "CITY", "STALP", "ASSET",
        "DEP", "NETINC", "ROA", "ROE", "ACTIVE",
    ] if c in institutions.columns]
    st.dataframe(
        institutions[display_cols],
        use_container_width=True,
        hide_index=True,
    )
