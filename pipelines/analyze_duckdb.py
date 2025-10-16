"""
Step 7: Data Exploration & Visualization
----------------------------------------
Explores the validated DuckDB database and creates charts
for GDP, CO₂, and Population trends.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ───────────────────────────────
# Setup
# ───────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "data-cleaning.duckdb"
REPORT_DIR = PROJECT_ROOT / "data" / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ───────────────────────────────
# Connect to DuckDB
# ───────────────────────────────
con = duckdb.connect(str(DB_PATH))
print(f" Connected to {DB_PATH}")

# ───────────────────────────────
# 1️⃣ Top 10 GDP Countries (Latest Year)
# ───────────────────────────────
top_gdp = con.execute("""
    SELECT country_name, year, gdp
    FROM clean_data
    WHERE year = (SELECT MAX(year) FROM clean_data)
    AND gdp IS NOT NULL
    ORDER BY gdp DESC
    LIMIT 10;
""").fetchdf()

plt.figure(figsize=(10,6))
plt.barh(top_gdp["country_name"], top_gdp["gdp"]/1e12)
plt.gca().invert_yaxis()
plt.xlabel("GDP (Trillions USD)")
plt.title(f"Top 10 GDP Countries – {int(top_gdp['year'].iloc[0])}")
plt.tight_layout()
plt.savefig(REPORT_DIR / "top10_gdp.png")
plt.close()
print(" Saved: top10_gdp.png")

# ───────────────────────────────
# 2️⃣ Global CO₂ Emissions Trend
# ───────────────────────────────
global_co2 = con.execute("""
    SELECT year, SUM(co2_emissions) AS total_co2
    FROM clean_data
    WHERE co2_emissions IS NOT NULL
    GROUP BY year
    ORDER BY year;
""").fetchdf()

plt.figure(figsize=(10,6))
plt.plot(global_co2["year"], global_co2["total_co2"]/1e6, marker="o")
plt.xlabel("Year")
plt.ylabel("Total CO₂ Emissions (Million Tons)")
plt.title("Global CO₂ Emissions Over Time")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(REPORT_DIR / "global_co2_trend.png")
plt.close()
print(" Saved: global_co2_trend.png")

# ───────────────────────────────
# 3️⃣ GDP vs CO₂ Relationship (Scatter)
# ───────────────────────────────
scatter_df = con.execute("""
    SELECT gdp, co2_emissions
    FROM clean_data
    WHERE gdp IS NOT NULL AND co2_emissions IS NOT NULL
    AND year >= 2000;
""").fetchdf()

plt.figure(figsize=(8,6))
plt.scatter(scatter_df["gdp"]/1e9, scatter_df["co2_emissions"], alpha=0.4)
plt.xlabel("GDP (Billions USD)")
plt.ylabel("CO₂ Emissions (kt)")
plt.title("GDP vs CO₂ Emissions (2000+)")
plt.tight_layout()
plt.savefig(REPORT_DIR / "gdp_vs_co2.png")
plt.close()
print(" Saved: gdp_vs_co2.png")

# ───────────────────────────────
# Wrap up
# ───────────────────────────────
con.close()
print("\n Visualization complete — charts saved to data/reports/")
