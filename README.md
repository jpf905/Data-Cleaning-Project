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




---

### **Technologies Used**
- **Python 3.13**
- **Prefect 2.x** – pipeline orchestration  
- **DuckDB** – lightweight analytical database  
- **Pandas** – data cleaning & merging  
- **Matplotlib / Plotly** – visualizations  
- **Streamlit** – interactive dashboard front-end  

---

### **Pipeline Flow**

| Step | Script | Description | Output |
|------|---------|-------------|---------|
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