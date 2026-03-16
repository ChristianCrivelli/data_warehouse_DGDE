import pyodbc
import pandas as pd
import numpy as np

# Connecting to the Databse
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;"
    , autocommit=True
)

cursor = conn.cursor()

# Loading the CRM Sales Table
df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_sales", con=conn)

## Fix date formating where order_date is not a valid date
incorrect_dates = df["order_date"] > df["shipping_date"]
df.loc[incorrect_dates, "order_date"] = np.nan

## Order with the same ID should have the same order date!
df['order_date'] = pd.to_datetime(df['order_date'], format='%Y%m%d')
df['order_date'] = df.groupby('ID')['order_date'].transform('min')

## Fixing the Sale Amount, Quantity and Price columns
mask_fix_sale = (df["sale_amount"] != df["price"]) & ((df["sale_amount"] <= 0) | (df["sale_amount"].isna()))
df.loc[mask_fix_sale, "sale_amount"] = df["price"]

mask_fix_price = (df["sale_amount"] != df["price"]) & ((df["price"] <= 0) | (df["price"].isna()))
df.loc[mask_fix_price, "price"] = df["sale_amount"]

# Logic to send the data back
columns = ", ".join(df.columns)
values_placeholders = ", ".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.crm_sales ({columns}) VALUES ({values_placeholders})"

cursor = conn.cursor()
cursor.fast_executemany = True
params = [tuple(x) for x in df.values]

try:
    cursor.executemany(insert_sql, params)
    conn.commit()
    print(f"Successfully inserted {len(df)} rows.")
except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()