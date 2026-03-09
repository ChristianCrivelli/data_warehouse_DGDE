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

# Trimming and other cleaning stuffs that I didn't have time to implement   