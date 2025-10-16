"""
Step 6: Automate the Data-Cleaning Pipeline with Prefect
--------------------------------------------------------
Runs all pipeline steps (end to end):
  Extract → Transform/Clean → Load → Validate
"""

from prefect import flow, task
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ───────────────────────────────
# Tasks – each stage is a task
# ───────────────────────────────
@task(name="Extract Data")
def extract():
    print(" Running Step 1: Extract")
    subprocess.run(["python", str(PROJECT_ROOT / "pipelines" / "extract_sources.py")], check=True)

@task(name="Transform & Clean Data")
def transform_clean():
    print(" Running Step 2: Transform + Clean")
    subprocess.run(["python", str(PROJECT_ROOT / "pipelines" / "transform_clean.py")], check=True)

@task(name="Load to DuckDB")
def load_to_duckdb():
    print(" Running Step 3: Load")
    subprocess.run(["python", str(PROJECT_ROOT / "pipelines" / "load_to_duckdb.py")], check=True)

@task(name="Validate Data")
def validate():
    print(" Running Step 4: Validate")
    subprocess.run(["python", str(PROJECT_ROOT / "pipelines" / "validate_data.py")], check=True)

# ───────────────────────────────
# Flow definition (ETL + Validate)
# ───────────────────────────────
@flow(name="Data-Cleaning Pipeline")
def data_cleaning_pipeline():
    extract()
    transform_clean()
    load_to_duckdb()
    validate()
    print(" Pipeline complete — all steps succeeded!")

# ───────────────────────────────
# Run locally
# ───────────────────────────────
if __name__ == "__main__":
    data_cleaning_pipeline()
