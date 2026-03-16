import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# Connecting to the Database
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;"
    , autocommit=True
)

cursor = conn.cursor()

engine = create_engine("mssql+pyodbc://localhost\\SQLEXPRESS/dedg_database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Loading the CRM Customer Product Table
df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_product", con=conn)

# Getting rid of Null and Duplicate IDs
df = df.dropna(subset=["ID"])
df = df.drop_duplicates(subset=["ID"], keep = "last")

# Splitting Product Key, into Product Key and product Category~
## Initiate Product_Category
df["product_category"] = df["product_key"].str[:5].str.replace("-", "_", regex=False)

## Move product_category to the 3rd column (Index 2)
cols = df.columns.tolist()
cols.insert(2, cols.pop(cols.index("product_category")))
df = df[cols]

## Clean Product Key
df["product_key"] = df["product_key"].str[6:]

# Logic to change the nulls to 0
df["product_cost"] = pd.to_numeric(df["product_cost"], errors='coerce').fillna(0)

# Persist "Product Line" for clarity
df["product_line"] = df["product_line"].str.replace("R", "Road")
df["product_line"] = df["product_line"].str.replace("S", "Sport")
df["product_line"] = df["product_line"].str.replace("M", "Mountain")
df["product_line"] = df["product_line"].str.replace("T", "Touring")
df["product_line"] = df["product_line"].replace("", None).fillna("N/A")

# Corrects the problem with Date (Incorrect end dates)
df["product_start_date"] = pd.to_datetime(df["product_start_date"], errors="coerce")
df["product_end_date"] = pd.to_datetime(df["product_end_date"], errors="coerce")

df = df.sort_values(by=["product_key", "ID"]).copy()

df["product_end_date"] = (
    df.groupby("product_key")["product_start_date"]
    .shift(-1) - pd.Timedelta(days=1)
)

df = df.sort_values(by=["ID"]).copy()
df["product_end_date"] = df["product_end_date"].dt.strftime("%Y-%m-%d")
df["product_end_date"] = df["product_end_date"].fillna(pd.NaT)

# Logic to send the data back
df.to_sql(name='crm_product', 
          schema='trasnformation', 
          con=engine, 
          if_exists='replace', 
          index=False)