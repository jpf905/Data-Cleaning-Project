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

data-cleaning/
├─ pipelines/
│ ├─ extract_sources.py
│ ├─ transform_clean.py
│ ├─ load_to_duckdb.py
│ ├─ validate_data.py
│ └─ flow.py
├─ app/
│ └─ streamlit_app.py
├─ data/
│ ├─ raw/
│ ├─ processed/
│ ├─ warehouse/
│ └─ reports/
└─ README.md


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

### **Running the Pipeline Locally**

1. **Set up environment**
```bash
conda create -n data-cleaning python=3.13 -y
conda activate data-cleaning
pip install -r requirements.txt
python pipelines/flow.py
streamlit run app/streamlit_app.py
