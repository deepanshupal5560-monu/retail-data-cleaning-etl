# Automated Retail Data Cleaning & SQL Validation Pipeline

An end-to-end Python and SQL ETL pipeline that ingests raw retail order CSV files, enforces data quality validation rules, isolates corrupt records into an audit error log, and loads clean records into a normalized SQLite relational database for downstream analytics.

## Pipeline Workflow
1. **Extract:** Ingests raw retail order records from CSV (`raw_retail_orders.csv`).
2. **Validate & Clean:**
   - Removes duplicate `order_id` entries.
   - Filters out missing customer names or categories and non-positive order amounts.
   - Standardizes text casing and normalizes date formats to `YYYY-MM-DD`.
   - Logs all invalid records with specific failure reasons into `rejected_records.csv`.
3. **Load:** Inserts validated records into normalized `customers` and `orders` tables in `retail_warehouse.db` using Primary/Foreign key constraints.
4. **Analyze:** Executes SQL `JOIN` and `GROUP BY` queries to generate category revenue and top-customer summaries.

## How to Run
```bash
pip install -r requirements.txt
python etl_pipeline.py
```