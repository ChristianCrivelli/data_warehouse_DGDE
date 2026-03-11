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

# Loading the ERP Customer Table
df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_customer", con=conn)

# Correct "NAS" in IDs

# Correct F's and M's in Gender and fill in Missing
df["gender"] = df["gender"].replace("M", "Male")
df["gender"] = df["gender"].replace("F", "Female")
df["gender"] = df["gender"].replace("", None).fillna("N/A")

# Logic to send the data back
columns = ", ".join(df.columns)
values_placeholders = ", ".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.erp_customer ({columns}) VALUES ({values_placeholders})"

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