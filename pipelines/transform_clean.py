"""
Step 3: Transform, Clean & Profile
Cleans Population, GDP, and CO₂ data, merges into one CSV,
and generates data-quality visuals.
"""

from pathlib import Path
import pandas as pd
import logging
import matplotlib.pyplot as plt

# ───────────────────────────────
# Setup
# ───────────────────────────────
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
REPORT_DIR = Path("data/reports")
LOG_DIR = Path("logs")

for d in [PROCESSED_DIR, REPORT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "transform_clean.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ───────────────────────────────
# Cleaning helpers (load_and_clean, merge_datasets)
# ───────────────────────────────
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def _standardize_long_format(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    df = _normalize_columns(df)
    if {"country_name", "year", "value"}.issubset(df.columns):
        out = df[["country_name", "year", "value"]].copy()
        out.rename(columns={"value": value_name}, inplace=True)
        return out
    if {"country", "year", "total"}.issubset(df.columns):
        out = df[["country", "year", "total"]].copy()
        out.rename(columns={"country": "country_name", "total": value_name}, inplace=True)
        return out
    if {"year", "total"}.issubset(df.columns) and "country" not in df.columns:
        out = df[["year", "total"]].copy()
        out.rename(columns={"total": value_name}, inplace=True)
        out["country_name"] = "World"
        return out[["country_name", "year", value_name]]
    raise KeyError(f"Unexpected columns: {list(df.columns)}")

def load_and_clean(path: Path, value_name: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _standardize_long_format(df, value_name)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df[value_name] = pd.to_numeric(df[value_name], errors="coerce")
    df = df.dropna(subset=["year", value_name])
    df["year"] = df["year"].astype(int)
    df["country_name"] = df["country_name"].astype(str).str.strip()
    return df[["country_name", "year", value_name]]

def merge_datasets() -> pd.DataFrame:
    pop_file = sorted(RAW_DIR.glob("population_*.csv"))[-1]
    gdp_file = sorted(RAW_DIR.glob("gdp_*.csv"))[-1]
    co2_file = sorted(RAW_DIR.glob("co2_emissions_*.csv"))[-1]
    pop = load_and_clean(pop_file, "population")
    gdp = load_and_clean(gdp_file, "gdp")
    co2 = load_and_clean(co2_file, "co2_emissions")
    df = pop.merge(gdp, on=["country_name", "year"], how="outer")
    df = df.merge(co2, on=["country_name", "year"], how="outer")
    for col in ["population", "gdp", "co2_emissions"]:
        if col in df.columns:
            df.loc[df[col] < 0, col] = pd.NA
            df[col] = df.groupby("country_name")[col].ffill()
    df = df.sort_values(["country_name", "year"])
    out_path = PROCESSED_DIR / "clean_data.csv"
    df.to_csv(out_path, index=False)
    print(f" Cleaned dataset saved to {out_path}")
    return df

# ───────────────────────────────
# 🔍 Data-quality visualizations
# ───────────────────────────────
def create_visuals(df: pd.DataFrame):
    """Lightweight data-quality summary + charts (no profiling package)."""
    summary_path = REPORT_DIR / "data_quality_summary.txt"

    # 1️⃣ Save quick stats
    with open(summary_path, "w") as f:
        f.write("=== Basic Dataset Summary ===\n")
        f.write(f"Rows: {len(df)}\nColumns: {list(df.columns)}\n\n")
        f.write("=== Missing Values ===\n")
        f.write(str(df.isna().sum()))
        f.write("\n\n=== Data Types ===\n")
        f.write(str(df.dtypes))
        f.write("\n\n=== Numeric Summary ===\n")
        f.write(str(df.describe().T))
    print(f" Text summary saved to {summary_path}")

    # 2️⃣ Missing-value heatmap
    plt.figure(figsize=(10, 5))
    plt.imshow(df.isna(), aspect="auto", interpolation="nearest", cmap="viridis")
    plt.title("Missing Values Heatmap")
    plt.xlabel("Columns")
    plt.ylabel("Rows")
    plt.tight_layout()
    plt.savefig(REPORT_DIR / "missing_heatmap.png")
    plt.close()

    # 3️⃣ Missing counts per column
    plt.figure(figsize=(6, 4))
    df.isna().sum().plot(kind="bar")
    plt.title("Missing Values per Column")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(REPORT_DIR / "missing_counts.png")
    plt.close()

    print(f" Charts saved to {REPORT_DIR}")

# ───────────────────────────────
# Main
# ───────────────────────────────
if __name__ == "__main__":
    df = merge_datasets()
    create_visuals(df)
    print(" Data profiling complete! Check data/reports/ for visuals.")
