"""
Step 4: Load Cleaned Data into DuckDB (final version)
-----------------------------------------------------
Creates data/warehouse/data-cleaning.duckdb from processed CSV.
"""

import duckdb
from pathlib import Path

# ───────────────────────────────
# Robust project-root path logic
# ───────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "data-cleaning.duckdb"

print(f" Project root detected: {PROJECT_ROOT}")
print(f" Input CSV: {DATA_PATH}")
print(f" Output DuckDB: {DB_PATH}")

# Ensure directories exist
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ───────────────────────────────
# Load CSV into DuckDB
# ───────────────────────────────
def load_to_duckdb(csv_path=DATA_PATH, db_path=DB_PATH, table_name="clean_data"):
    if not csv_path.exists():
        raise FileNotFoundError(f" CSV file not found: {csv_path.resolve()}")
    print(f" Loading {csv_path.name} into {db_path.name} ...")

    con = duckdb.connect(str(db_path))
    con.execute(f"DROP TABLE IF EXISTS {table_name};")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}');")

    count = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
    print(f" Loaded {count:,} rows into table '{table_name}'")

    sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5;").fetchdf()
    print("\n Sample rows:")
    print(sample)
    con.close()

    print(f"\n Database created at: {db_path.resolve()}")

if __name__ == "__main__":
    load_to_duckdb()
