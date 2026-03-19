import pyodbc
import pandas as pd
import numpy as np

# 1. Establish Connection
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;",
    autocommit=True
)
cursor = conn.cursor()

try:
    # 2. Extract Data
    df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_sales", con=conn)

    # 3. Clean Dates (Process as Pandas Datetime objects first)
    df['order_date'] = pd.to_datetime(df['order_date'], format='%Y%m%d', errors='coerce')
    df['shipping_date'] = pd.to_datetime(df['shipping_date'], format='%Y%m%d', errors='coerce')

    # Logic: order_date cannot be after shipping_date
    incorrect_dates = df["order_date"] > df["shipping_date"]
    df.loc[incorrect_dates, "order_date"] = pd.NaT

    # Logic: Same ID should have the same minimum order date
    df['order_date'] = df.groupby('ID')['order_date'].transform('min')

    # 4. Clean Numerics
    df['sale_amount'] = pd.to_numeric(df['sale_amount'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # Fix mismatches between sale_amount and price
    mask_fix_sale = (df["sale_amount"] != df["price"]) & ((df["sale_amount"] <= 0) | (df["sale_amount"].isna()))
    df.loc[mask_fix_sale, "sale_amount"] = df["price"]

    mask_fix_price = (df["sale_amount"] != df["price"]) & ((df["price"] <= 0) | (df["price"].isna()))
    df.loc[mask_fix_price, "price"] = df["sale_amount"]

    # ==========================================
    # --- CRITICAL FIXES FOR PyODBC INSERTION ---
    # ==========================================

    # Fix A: Convert all Pandas Timestamps to SQL-friendly strings (YYYY-MM-DD)
    df['order_date'] = df['order_date'].dt.strftime('%Y-%m-%d')
    df['shipping_date'] = df['shipping_date'].dt.strftime('%Y-%m-%d')
    
    # NOTE: Your sample data had a 6th column with '20110110'. If that is a date column (e.g., 'due_date'), format it too:
    if 'due_date' in df.columns: 
         df['due_date'] = pd.to_datetime(df['due_date'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')

    # Fix B: Ensure quantity (the '1' in your sample) is actually a number, not a string
    if 'quantity' in df.columns: 
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    # Fix C: Align DataFrame columns EXACTLY with SQL Server table schema order
    target_cols_query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'crm_sales' AND TABLE_SCHEMA = 'transformation' 
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(target_cols_query)
    target_columns = [row[0] for row in cursor.fetchall()]
    
    # Reorder df to match SQL table exactly
    df = df[target_columns]

    # Fix D: Safely convert NaN/NaT to Python None (which translates perfectly to SQL NULL)
    df = df.astype(object).where(pd.notnull(df), None)

    # 5. Load Data back to SQL
    columns_str = ", ".join(target_columns)
    placeholders = ", ".join(["?"] * len(target_columns))
    insert_sql = f"INSERT INTO transformation.crm_sales ({columns_str}) VALUES ({placeholders})"

    # Generate parameter tuples
    params = [tuple(row) for row in df.values]

    # Execute
    cursor.fast_executemany = True
    cursor.executemany(insert_sql, params)
    print(f"Success! Inserted {len(df)} rows.")

except Exception as e:
    print(f"An error occurred: {e}")
    # This will print the exact tuple that failed if it crashes again
    if 'params' in locals() and len(params) > 0:
         print(f"Sample data that failed: {params[0]}")
finally:
    cursor.close()
    conn.close()