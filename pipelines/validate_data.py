"""
Step 5: Validate Data Quality in DuckDB
---------------------------------------
Performs automated validation checks on the cleaned dataset
stored in data/warehouse/data-cleaning.duckdb.
"""

import duckdb
import pandas as pd
from pathlib import Path

# Use absolute path relative to the project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "data-cleaning.duckdb"

# ───────────────────────────────
# Connection
# ───────────────────────────────
def connect_duckdb(db_path: Path = DB_PATH):
    """Connect to the local DuckDB database file."""
    if not db_path.exists():
        raise FileNotFoundError(f" DuckDB file not found: {db_path.resolve()}")
    con = duckdb.connect(str(db_path))
    print(f" Connected to {db_path}")
    return con

# ───────────────────────────────
# Validation checks
# ───────────────────────────────
def validate_basic(con):
    """Confirm table exists and has rows."""
    tables = con.execute("SHOW TABLES").fetchdf()
    if "clean_data" not in tables["name"].values:
        raise RuntimeError(" Table 'clean_data' not found in database.")
    row_count = con.execute("SELECT COUNT(*) FROM clean_data;").fetchone()[0]
    assert row_count > 0, " No rows found in table!"
    print(f" Row count: {row_count:,}")

def validate_schema(con):
    """Ensure expected columns exist."""
    df = con.execute("DESCRIBE clean_data;").fetchdf()
    expected = {"country_name", "year", "population", "gdp", "co2_emissions"}
    found = set(df["column_name"].tolist())
    missing = expected - found
    assert not missing, f" Missing columns: {missing}"
    print(" All expected columns present")

def validate_missing_values(con):
    """Count missing values and ensure they are within acceptable limits."""
    missing_df = con.execute("""
        SELECT 
            SUM(population IS NULL) AS missing_population,
            SUM(gdp IS NULL) AS missing_gdp,
            SUM(co2_emissions IS NULL) AS missing_co2
        FROM clean_data;
    """).fetchdf()
    print("\n Missing values summary:")
    print(missing_df.to_string(index=False))

    total = con.execute("SELECT COUNT(*) FROM clean_data;").fetchone()[0]
    gdp_missing = missing_df.loc[0, "missing_gdp"]
    assert gdp_missing / total < 0.2, f" Too many missing GDP values: {gdp_missing/total:.1%}"
    print(" Missing-value ratios acceptable")

def validate_ranges(con):
    """Validate numeric ranges for sanity."""
    stats = con.execute("""
        SELECT 
            MIN(year) AS min_year, MAX(year) AS max_year,
            MIN(population) AS min_pop, MAX(population) AS max_pop,
            MIN(gdp) AS min_gdp, MAX(gdp) AS max_gdp,
            MIN(co2_emissions) AS min_co2, MAX(co2_emissions) AS max_co2
        FROM clean_data;
    """).fetchdf()
    print("\n Numeric summary:")
    print(stats.T)
    
    min_year, max_year = int(stats.loc[0, "min_year"]), int(stats.loc[0, "max_year"])
    assert 1700 <= min_year <= 2100, f" Year range invalid: {min_year}"
    assert 1700 <= max_year <= 2100, f" Year range invalid: {max_year}"
    print(" Year range valid")
    
def validate_correlations(con):
    """Check correlation between GDP and CO₂ emissions."""
    corr = con.execute("""
        SELECT corr(gdp, co2_emissions) AS corr_value
        FROM clean_data
        WHERE gdp IS NOT NULL AND co2_emissions IS NOT NULL;
    """).fetchone()[0]

    if corr is None:
        print("\n Not enough data to compute correlation.")
        return

    print(f"\n GDP–CO₂ correlation: {corr:.3f}")
    if corr < 0:
        print(" Negative correlation detected (unexpected pattern)")
    else:
        print(" Positive correlation (plausible economic relationship)")

# ───────────────────────────────
# Main
# ───────────────────────────────
def main():
    con = connect_duckdb()
    validate_basic(con)
    validate_schema(con)
    validate_missing_values(con)
    validate_ranges(con)
    validate_correlations(con)
    con.close()
    print("\n Validation complete — data quality confirmed!")

if __name__ == "__main__":
    main()
    