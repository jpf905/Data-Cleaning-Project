## **Data-Cleaning Pipeline & Dashboard**

### **Overview**
This project demonstrates a **complete data-engineering and data-science workflow** — from raw data collection to a fully validated and visualized dataset.
Using **Prefect**, **DuckDB**, and **Streamlit**, the pipeline automates:
> **Extract → Transform → Load → Validate → Visualize**

The result is a clean, reproducible ETL process and an interactive dashboard for exploring GDP, CO₂ emissions, and population trends.

### **Summary**
* Built a modular, automated ETL pipeline with Prefect
* Implemented data quality validation using DuckDB SQL
* Delivered interactive visualizations via Streamlit
* Demonstrated skills in data engineering and data science

---

### **Architecture**

```
data-cleaning/
├─ pipelines/
│  ├─ extract_sources.py
│  ├─ transform_clean.py
│  ├─ load_to_duckdb.py
│  ├─ validate_data.py
│  └─ flow.py
├─ app/
│  └─ streamlit_app.py
├─ data/
│  ├─ raw/
│  ├─ processed/
│  ├─ warehouse/
│  └─ reports/
└─ README.md
```



---

### **Technologies Used**
- **Python 3.13**
- **Prefect 2.x** – pipeline orchestration  
- **DuckDB** – lightweight analytical database  
- **Pandas** – data cleaning & merging  
- **Matplotlib / Plotly** – visualizations  
- **Streamlit** – interactive dashboard front-end  

---


--- ### **Data**

The raw data for this project was collected from multiple open sources, primarily the World Bank Open Data (for GDP and population indicators) and Our World in Data (for CO₂ emissions data). Each dataset initially presented significant inconsistencies and formatting issues — such as differing country names, irregular year ranges, and missing or placeholder values (e.g., "..", "N/A"). Numeric columns were stored in varying units and scales, while some records included duplicated or incomplete entries. These discrepancies made it impossible to merge the datasets directly and required careful data cleaning, normalization, and schema alignment before integration into the unified analytical model.

After processing through the transformation and validation pipeline, the data was consolidated into a single, standardized dataset stored in data/processed/clean_data.csv and loaded into DuckDB for analysis. All three sources — GDP, population, and CO₂ emissions — were normalized to share consistent country identifiers, year formats, and numeric scales. Missing values were handled systematically, out-of-range years were filtered, and duplicates were removed to ensure referential integrity. Column names were standardized to a unified schema (country_name, year, population, gdp, co2_emissions), enabling seamless SQL queries and reproducible analytics. The result is a clean, validated dataset that supports accurate cross-country and longitudinal analysis within the interactive dashboard.

---

### **Pipeline Flow**

| Step | Script | Description | Output |
|------|---------|-------------|---------|
| **1. Collect & Design Schema** | *(no script)* | Collect 2–3 related messy datasets (e.g., population, GDP, education). Normalize into a relational schema (e.g., countries, indicators, metrics). | `data/raw/` and schema plan |
| **2. Extract** | `extract_sources.py` | Downloads raw CSVs | `data/raw/` |
| **3. Transform + Clean** | `transform_clean.py` | Cleans, merges, validates schema | `data/processed/clean_data.csv` |
| **4. Load** | `load_to_duckdb.py` | Loads data into DuckDB | `data/warehouse/data-cleaning.duckdb` |
| **5. Validate** | `validate_data.py` | Automated QA checks | `data/reports/data_quality_summary.txt` |
| **6. Automate** | `flow.py` | Prefect flow (end-to-end) | One-click ETL run |
| **7. Visualize** | `streamlit_app.py` | Interactive dashboard | http://localhost:8501 |
---

### **Photos**

### Streamlit Dashboard
![Dashboard Screenshot](https://github.com/jpf905/Data-Cleaning-Project/blob/main/screenshots/dashboard.png)

### Prefect Pipeline Run 1
![Prefect Pipeline Screenshot1](https://github.com/jpf905/Data-Cleaning-Project/blob/main/screenshots/pipeline_run1.png)

### Prefect Pipeline Run 2
![Prefect Pipeline Screenshot2](https://github.com/jpf905/Data-Cleaning-Project/blob/main/screenshots/pipeline_run2.png)

### Data Validation Summary
![Validation Screenshot](https://github.com/jpf905/Data-Cleaning-Project/blob/main/screenshots/validation_passed.png)