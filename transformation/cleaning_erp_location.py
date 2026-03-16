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


# Loading the ERP Table
df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_location", con=conn)

# Correcting Country naming convention 
## Stripping
df["country"] = df['country'].str.strip()

## Note: DE is Germany
df["country"] = df["country"].replace("DE", "Germany")

df["country"] = df["country"].replace("US", "United States of America")
df["country"] = df["country"].replace("USA", "United States of America")
df["country"] = df["country"].replace("United States", "United States of America")

## Cleaning Missing Values
df["country"] = df["country"].replace("", None).fillna("N/A")


# Logic to send the data back
columns = ", ".join(df.columns)
values_placeholders = ", ".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.erp_location ({columns}) VALUES ({values_placeholders})"

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