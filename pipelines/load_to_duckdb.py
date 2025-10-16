"""
Simplest possible loader: reads the cleaned CSV and stores it
in a local DuckDB database file.
"""

import duckdb
from pathlib import Path

DATA_PATH = Path("data/processed/clean_data.csv")
DB_PATH = Path("data/warehouse/chaos_to_clean.duckdb")

# Check paths first
if not DATA_PATH.exists():
    raise FileNotFoundError(f"❌ CSV not found: {DATA_PATH.resolve()}")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

print(f"📥 Loading '{DATA_PATH}' into '{DB_PATH}' ...")

# Connect (creates DB file if needed)
con = duckdb.connect(str(DB_PATH))

# Drop & recreate the table
con.execute("DROP TABLE IF EXISTS clean_data;")
con.execute(f"CREATE TABLE clean_data AS SELECT * FROM read_csv_auto('{DATA_PATH}');")

# Verify row count
count = con.execute("SELECT COUNT(*) FROM clean_data;").fetchone()[0]
print(f"✅ Loaded {count} rows into 'clean_data'")

# Show a quick preview
sample = con.execute("SELECT * FROM clean_data LIMIT 5;").fetchdf()
print("\n🔎 Sample rows:")
print(sample)

con.close()
print("\n🏁 Done! DuckDB saved at:", DB_PATH.resolve())
