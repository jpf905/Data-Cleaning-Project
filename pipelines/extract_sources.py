"""
Extract public datasets (CSV files) and store them in data/raw/.
Works with verified open data URLs (no API keys needed).
"""

import os
import requests
import logging
from pathlib import Path
from datetime import datetime

# -----------------------------
# Setup directories and logging
# -----------------------------
RAW_DIR = Path("data/raw")
LOG_DIR = Path("logs")

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "extraction.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Verified working data sources
# -----------------------------
DATASETS = {
    "population": "https://datahub.io/core/population/r/population.csv",
    "gdp": "https://datahub.io/core/gdp/r/gdp.csv",
    "co2_emissions": "https://datahub.io/core/co2-fossil-global/r/global.csv"
}

# -----------------------------
# Helper: download a single CSV
# -----------------------------
def download_csv(name, url):
    """Download a CSV file and save it to data/raw/."""
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        filename = f"{name}_{datetime.now().strftime('%Y%m%d')}.csv"
        dest_path = RAW_DIR / filename

        with open(dest_path, "wb") as f:
            f.write(response.content)

        logging.info(f"s Saved {name} to {dest_path} ({len(response.content)/1024:.1f} KB)")
        print(f" Downloaded: {name}")
        return dest_path

    except Exception as e:
        logging.error(f"❌ Failed to download {name}: {e}")
        print(f"❌ Failed to download {name}: {e}")
        return None

# -----------------------------
# Main extraction routine
# -----------------------------
def extract_all():
    """Download all datasets defined in DATASETS."""
    print("Starting data extraction...\n")
    for name, url in DATASETS.items():
        download_csv(name, url)
    print("\nAll downloads completed! Check data/raw/")

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    extract_all()
