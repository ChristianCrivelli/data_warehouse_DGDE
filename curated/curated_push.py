import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# Database Connection Configuration
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")

# --- 1. DIM_CUSTOMERS ---
customer_crm_df = pd.read_sql("SELECT * FROM transformation.crm_customer", engine)
customer_erp_df = pd.read_sql("SELECT * FROM transformation.erp_customer", engine)
location_erp_df = pd.read_sql("SELECT * FROM transformation.erp_location", engine)

# FIX: Convert ID columns to strings to ensure they match for the merge
customer_crm_df["ID"] = customer_crm_df["ID"].astype(str)
customer_erp_df["ID"] = customer_erp_df["ID"].astype(str)
location_erp_df["ID"] = location_erp_df["ID"].astype(str)

# Join CRM and ERP customers on ID
df_cust = pd.merge(
    left=customer_crm_df,
    right=customer_erp_df,
    how="left",
    on="ID",
    suffixes=('', '_erp')
)

# Join with Location data
df_cust = pd.merge(
    left=df_cust,
    right=location_erp_df,
    how="left",
    on="ID",
    suffixes=("", "_loc")
)

dim_customers = pd.DataFrame({
    "customer_id": df_cust["ID"],
    "customer_number": df_cust["customer_key"],
    "first_name": df_cust["first_name"],
    "last_name": df_cust["last_name"],
    "country": df_cust["country"],
    "marital_status": df_cust["marital_status"],
    "gender": df_cust["gender"],       
    "birthdate": df_cust["date_of_birth"],
    "create_date": df_cust["date_of_creation"]
})

dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
dim_customers.insert(0, "customer_key", dim_customers.index + 1)

dim_customers.to_sql(name="dim_customers", con=engine, schema="curated", if_exists="replace", index=False)
print("✅ dim_customers loaded into curated.dim_customers")


# --- 2. DIM_PRODUCTS ---
product_crm_df = pd.read_sql("SELECT * FROM transformation.crm_product", engine)
category_erp_df = pd.read_sql("SELECT * FROM transformation.erp_product_category", engine)

# Ensure keys are strings for safety
product_crm_df["product_category"] = product_crm_df["product_category"].astype(str)
category_erp_df["ID"] = category_erp_df["ID"].astype(str)

df_prod = pd.merge(
    left=product_crm_df,
    right=category_erp_df,
    how="left",
    left_on="product_category",
    right_on="ID"
)

dim_products = pd.DataFrame({
    "product_number": df_prod["product_key"],
    "product_name": df_prod["product_name"],
    "category_id": df_prod["product_category"],
    "category": df_prod["category"],
    "subcategory": df_prod["subcategory"],
    "maintenance": df_prod["requires_maintenance"],
    "cost": df_prod["product_cost"],
    "product_line": df_prod["product_line"],
    "start_date": df_prod["product_start_date"],
    "end_date": df_prod["product_end_date"]
})

dim_products = dim_products.sort_values("product_number").reset_index(drop=True)
dim_products.insert(0, "product_key", dim_products.index + 1)

dim_products.to_sql(name="dim_products", con=engine, schema="curated", if_exists="replace", index=False)
print("✅ dim_products loaded into curated.dim_products")


# --- 3. FACT_SALES ---
sales_details_df = pd.read_sql("SELECT * FROM transformation.crm_sales", engine)
dim_products_df = pd.read_sql("SELECT * FROM curated.dim_products", engine)
dim_customers_df = pd.read_sql("SELECT * FROM curated.dim_customers", engine)

# Ensure keys match between cleaned sales and the new curated dimensions
sales_details_df["product_key"] = sales_details_df["product_key"].astype(str)
dim_products_df["product_number"] = dim_products_df["product_number"].astype(str)
sales_details_df["customer_id"] = sales_details_df["customer_id"].astype(str)
dim_customers_df["customer_id"] = dim_customers_df["customer_id"].astype(str)

df_sales = pd.merge(
    left=sales_details_df,
    right=dim_products_df[["product_key", "product_number"]],
    how="left",
    left_on="product_key",
    right_on="product_number"
)

df_sales = pd.merge(
    left=df_sales,
    right=dim_customers_df[["customer_key", "customer_id"]],
    how="left",
    on="customer_id"
)

fact_sales = pd.DataFrame({
    "product_key": df_sales["product_key_y"], # The surrogate key from dim_products
    "customer_key": df_sales["customer_key"], # The surrogate key from dim_customers
    "order_number": df_sales["ID"],
    "order_date": df_sales["order_date"],
    "shipping_date": df_sales["shipping_date"],
    "due_date": df_sales["due_date"],
    "sales_amount": df_sales["sale_amount"],
    "quantity": df_sales["quantity"],
    "price": df_sales["price"]
})

fact_sales.insert(0, "sales_key", fact_sales.index + 1)
fact_sales.to_sql(name="fact_sales", con=engine, schema="curated", if_exists="replace", index=False)
print("✅ fact_sales loaded into curated.fact_sales")