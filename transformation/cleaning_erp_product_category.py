import pyodbc
import pandas as pd

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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_product_category", con=conn)

# Stripping "requires_maintenance"
df["requires_maintenance"] = df['requires_maintenance'].str.strip()

## Making "requires_maintenance" to be a boolean
df["requires_maintenance"] = df["requires_maintenance"].replace("YES", 1)
df["requires_maintenance"] = df["requires_maintenance"].replace("NO", 0)

# Logic to send the data back
columns = ", ".join(df.columns)
values_placeholders = ", ".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.erp_product_category ({columns}) VALUES ({values_placeholders})"

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