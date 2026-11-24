from datetime import datetime
import sqlite3
import pandas as pd

RAW_FILE = "raw_retail_orders.csv"
CLEAN_FILE = "cleaned_retail_orders.csv"
REJECTED_FILE = "rejected_records.csv"
DB_FILE = "retail_warehouse.db"


def create_sample_raw_data():
  """Generates a realistic messy CSV dataset for testing the ETL pipeline."""
  raw_rows = [
      {
          "order_id": "ORD-1001",
          "customer_id": "CUST-01",
          "customer_name": "  rahul sharma ",
          "city": "delhi",
          "category": "electronics",
          "order_amount": 45000.0,
          "order_date": "2024-03-10",
      },
      {
          "order_id": "ORD-1002",
          "customer_id": "CUST-02",
          "customer_name": "Priya Verma",
          "city": "Noida",
          "category": "home appliances",
          "order_amount": 12500.5,
          "order_date": "2024-03-11",
      },
      {
          "order_id": "ORD-1001",
          "customer_id": "CUST-01",
          "customer_name": "Rahul Sharma",
          "city": "Delhi",
          "category": "Electronics",
          "order_amount": 45000.0,
          "order_date": "2024-03-10",
      },  # Duplicate order_id
      {
          "order_id": "ORD-1003",
          "customer_id": "CUST-03",
          "customer_name": "",
          "city": "Ghaziabad",
          "category": "Fashion",
          "order_amount": 3200.0,
          "order_date": "2024-03-12",
      },  # Missing name
      {
          "order_id": "ORD-1004",
          "customer_id": "CUST-04",
          "customer_name": "Amit Kumar",
          "city": "lucknow",
          "category": "electronics",
          "order_amount": -1500.0,
          "order_date": "2024-03-13",
      },  # Negative amount
      {
          "order_id": "ORD-1005",
          "customer_id": "CUST-02",
          "customer_name": "Priya Verma",
          "city": "Noida",
          "category": "Electronics",
          "order_amount": 18900.0,
          "order_date": "2024/03/14",
      },  # Slash date format
      {
          "order_id": "ORD-1006",
          "customer_id": "CUST-05",
          "customer_name": "Neha Singh",
          "city": "Delhi",
          "category": "Groceries",
          "order_amount": 2450.0,
          "order_date": "invalid-date",
      },  # Invalid date
      {
          "order_id": "ORD-1007",
          "customer_id": "CUST-06",
          "customer_name": "Vikram Rao",
          "city": "Gurugram",
          "category": "fashion",
          "order_amount": 5600.0,
          "order_date": "2024-03-15",
      },
      {
          "order_id": "ORD-1008",
          "customer_id": "CUST-03",
          "customer_name": "Suresh Das",
          "city": "Ghaziabad",
          "category": "Groceries",
          "order_amount": None,
          "order_date": "2024-03-16",
      },  # Null amount
      {
          "order_id": "ORD-1009",
          "customer_id": "CUST-07",
          "customer_name": "Anjali Mehta",
          "city": "noida",
          "category": "Home Appliances",
          "order_amount": 8900.0,
          "order_date": "2024-03-17",
      },
  ]
  df = pd.DataFrame(raw_rows)
  df.to_csv(RAW_FILE, index=False)
  print(f"[STEP 1] Raw data generated: {RAW_FILE} ({len(df)} records)")


def extract_transform_validate():
  """Reads raw CSV, cleans valid rows, and isolates bad records into rejected_records.csv."""
  df = pd.read_csv(RAW_FILE)
  valid_records = []
  rejected_records = []
  seen_order_ids = set()

  for _, row in df.iterrows():
    record = row.to_dict()
    order_id = str(record.get("order_id", "")).strip()

    # Rule 1: Duplicate Order ID check
    if order_id in seen_order_ids:
      record["rejection_reason"] = "Duplicate Order ID"
      rejected_records.append(record)
      continue

    # Rule 2: Missing mandatory fields check
    cust_name = (
        str(record.get("customer_name", "")).strip()
        if pd.notna(record.get("customer_name"))
        else ""
    )
    category = (
        str(record.get("category", "")).strip()
        if pd.notna(record.get("category"))
        else ""
    )
    if not cust_name or not category:
      record["rejection_reason"] = "Missing Customer Name or Category"
      rejected_records.append(record)
      continue

    # Rule 3: Numeric & positive order_amount check
    amount = record.get("order_amount")
    if pd.isna(amount) or float(amount) <= 0:
      record["rejection_reason"] = "Invalid or Non-Positive Order Amount"
      rejected_records.append(record)
      continue

    # Rule 4: Date standardization (YYYY-MM-DD)
    raw_date = str(record.get("order_date", "")).strip()
    parsed_date = None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
      try:
        parsed_date = datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
        break
      except ValueError:
        continue

    if not parsed_date:
      record["rejection_reason"] = "Invalid Date Format"
      rejected_records.append(record)
      continue

    # Standardize clean data
    seen_order_ids.add(order_id)
    valid_records.append({
        "order_id": order_id,
        "customer_id": str(record["customer_id"]).strip().upper(),
        "customer_name": cust_name.title(),
        "city": (
            str(record.get("city", "Unknown")).strip().title()
            if pd.notna(record.get("city"))
            else "Unknown"
        ),
        "category": category.title(),
        "order_amount": round(float(amount), 2),
        "order_date": parsed_date,
    })

  clean_df = pd.DataFrame(valid_records)
  rejected_df = pd.DataFrame(rejected_records)

  clean_df.to_csv(CLEAN_FILE, index=False)
  rejected_df.to_csv(REJECTED_FILE, index=False)
  print(
      f"[STEP 2] Validation Complete -> Valid: {len(clean_df)} | Rejected:"
      f" {len(rejected_df)}"
  )
  return clean_df


def load_to_sqlite(clean_df):
  """Loads cleaned data into normalized SQLite tables (customers & orders)."""
  conn = sqlite3.connect(DB_FILE)
  cursor = conn.cursor()

  cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            city TEXT
        )
    """)

  cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            category TEXT NOT NULL,
            order_amount REAL NOT NULL,
            order_date TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
    """)

  for _, row in clean_df.iterrows():
    cursor.execute(
        """
            INSERT OR REPLACE INTO customers (customer_id, customer_name, city)
            VALUES (?, ?, ?)
        """,
        (row["customer_id"], row["customer_name"], row["city"]),
    )

    cursor.execute(
        """
            INSERT OR REPLACE INTO orders (order_id, customer_id, category, order_amount, order_date)
            VALUES (?, ?, ?, ?, ?)
        """,
        (
            row["order_id"],
            row["customer_id"],
            row["category"],
            row["order_amount"],
            row["order_date"],
        ),
    )

  conn.commit()
  print(f"[STEP 3] Cleaned data successfully loaded into {DB_FILE}")
  return conn


def run_sql_analytics(conn):
  """Runs analytical SQL queries to generate downstream business reports."""
  print("\n--- SQL ANALYTICS REPORT: REVENUE BY CATEGORY ---")
  query_category = """
        SELECT 
            category AS Category,
            COUNT(order_id) AS Total_Orders,
            SUM(order_amount) AS Total_Revenue,
            ROUND(AVG(order_amount), 2) AS Avg_Order_Value
        FROM orders
        GROUP BY category
        ORDER BY Total_Revenue DESC;
    """
  report_df = pd.read_sql_query(query_category, conn)
  print(report_df.to_string(index=False))

  print("\n--- SQL ANALYTICS REPORT: TOP CUSTOMERS (JOIN QUERY) ---")
  query_customers = """
        SELECT 
            c.customer_name AS Customer,
            c.city AS City,
            COUNT(o.order_id) AS Orders_Count,
            SUM(o.order_amount) AS Total_Spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name, c.city
        ORDER BY Total_Spent DESC;
    """
  cust_df = pd.read_sql_query(query_customers, conn)
  print(cust_df.to_string(index=False))
  conn.close()


if __name__ == "__main__":
  create_sample_raw_data()
  cleaned_data = extract_transform_validate()
  db_conn = load_to_sqlite(cleaned_data)
  run_sql_analytics(db_conn)
  