"""
Streamlit Dashboard for Data-Cleaning Project
---------------------------------------------
Explores the validated dataset stored in data/warehouse/data-cleaning.duckdb.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# ───────────────────────────────
# Setup
# ───────────────────────────────
st.set_page_config(page_title="Global Data Dashboard", layout="wide")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "data-cleaning.duckdb"

# ───────────────────────────────
# Connect and load data
# ───────────────────────────────
@st.cache_data
def load_data():
    con = duckdb.connect(str(DB_PATH))
    df = con.execute("SELECT * FROM clean_data;").fetchdf()
    con.close()
    return df

df = load_data()

st.title(" Global Data Explorer")
st.caption("Data from validated DuckDB database — GDP, Population, and CO₂ emissions")

# ───────────────────────────────
# Sidebar filters
# ───────────────────────────────
years = sorted(df["year"].dropna().unique())
countries = sorted(df["country_name"].dropna().unique())

st.sidebar.header("Filters")
year_sel = st.sidebar.slider("Select Year", int(min(years)), int(max(years)), int(max(years)))
country_sel = st.sidebar.multiselect("Select Countries", countries, default=countries[:5])

filtered = df[(df["year"] == year_sel) & (df["country_name"].isin(country_sel))]

# ───────────────────────────────
# Layout: 3 columns
# ───────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Population", f"{filtered['population'].sum():,.0f}")
with col2:
    st.metric("Total GDP (USD)", f"{filtered['gdp'].sum():,.0f}")
with col3:
    st.metric("Total CO₂ (kt)", f"{filtered['co2_emissions'].sum():,.0f}")

st.divider()

# ───────────────────────────────
# Chart 1: Top 10 GDP countries (selected year)
# ───────────────────────────────
top_gdp = (
    df[df["year"] == year_sel]
    .sort_values("gdp", ascending=False)
    .head(10)
)
fig1 = px.bar(top_gdp, x="gdp", y="country_name",
              orientation="h", title=f"Top 10 GDP Countries ({year_sel})",
              labels={"gdp": "GDP (USD)", "country_name": "Country"})
st.plotly_chart(fig1, use_container_width=True)

# ───────────────────────────────
# Chart 2: Global CO₂ emissions trend
# ───────────────────────────────
global_co2 = (
    df.groupby("year", as_index=False)["co2_emissions"].sum()
    .sort_values("year")
)
fig2 = px.line(global_co2, x="year", y="co2_emissions",
               title="Global CO₂ Emissions Over Time",
               labels={"co2_emissions": "CO₂ (kt)", "year": "Year"})
st.plotly_chart(fig2, use_container_width=True)

# ───────────────────────────────
# Chart 3: GDP vs CO₂ scatter
# ───────────────────────────────
fig3 = px.scatter(
    df[df["year"] >= 2000],
    x="gdp", y="co2_emissions",
    color="year",
    title="GDP vs CO₂ Emissions (2000+)",
    labels={"gdp": "GDP (USD)", "co2_emissions": "CO₂ (kt)"}
)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ───────────────────────────────
# Data preview
# ───────────────────────────────
st.subheader("Raw Data Preview")
st.dataframe(df.head(20))
