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


# Loading the CRM Customer Info Table
df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_customer", con=conn)

# Getting rid of Null and Duplicate IDs
df = df.dropna(subset=["ID"])
df = df.drop_duplicates(subset=["ID"], keep = "last")

# Trimming relevant 
df["first_name"] = df['first_name'].str.strip()
df["last_name"] = df['last_name'].str.strip()

df["marital_status"] = df["marital_status"].replace("S", "Single")
df["marital_status"] = df["marital_status"].replace("M", "Married")
df["marital_status"] = df["marital_status"].fillna("N/A")

df["gender"] = df["gender"].replace("M", "Male")
df["gender"] = df["gender"].replace("F", "Female")
df["gender"] = df["gender"].replace("", None).fillna("N/A")

df["date_of_creation"] = pd.to_datetime(df["date_of_creation"])
df["ID"] = pd.to_numeric(df["ID"], errors='coerce').astype('Int64')

# Logic to send the data back
columns = ", ".join(df.columns)
values_placeholders = ", ".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.crm_customer ({columns}) VALUES ({values_placeholders})"

cursor.fast_executemany = True

# Convert Pandas NA/NAType objects to standard Python None for pyodbc compatibility
df_to_insert = df.astype(object).where(pd.notnull(df), None)

params = [tuple(x) for x in df_to_insert.values]
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