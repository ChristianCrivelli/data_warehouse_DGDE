import pyodbc

# Connecting to the Databse
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;"
    , autocommit=True
)

cursor = conn.cursor()

# Creating the Schema
cursor.execute("CREATE SCHEMA ingestion")

# Creating the Tables
## erp_customer table (CUST_AZ12.csv)
cursor.execute("""
    CREATE TABLE ingestion.erp_customer (
        ID NVARCHAR(50),
        date_of_birth NVARCHAR(20),
        gender NVARCHAR(10)
    )
    """)

## erp_location table (LOC_A101.csv)
cursor.execute("""
    CREATE TABLE ingestion.erp_location (
        ID NVARCHAR(50),
        country NVARCHAR(50)
    )
    """)

## erp_product_category table (PX_CAT_G1V2.csv)
cursor.execute("""
    CREATE TABLE ingestion.erp_product_category (
        ID NVARCHAR(10),
        category NVARCHAR(50),
        subcategory NVARCHAR(75),
        requires_maintence NVARCHAR(50)
    )
    """)

## crm_customer table (cust_info.csv)
cursor.execute("""
    CREATE TABLE ingestion.crm_customer (
        ID NVARCHAR(10),
        costumer_key NVARCHAR(50),
        first_name NVARCHAR(50),
        last_name NVARCHAR(50),
        marital_status NVARCHAR(5),
        gender NVARCHAR(5),
        date_of_creation NVARCHAR(50)
    )
    """)

## crm_product table (prd_info.csv)
cursor.execute("""
    CREATE TABLE ingestion.crm_product (
        ID NVARCHAR(5),
        product_key NVARCHAR(50),
        product_name NVARCHAR(100),      
        product_cost NVARCHAR(5),
        product_line NVARCHAR(5),
        product_start_date NVARCHAR(20),
        product_end_date NVARCHAR(20)
    )
    """)

## crm_sales table (sales_details.csv)
cursor.execute("""
    CREATE TABLE ingestion.crm_sales (
        ID NVARCHAR(50),
        product_key NVARCHAR(50),
        customer_id NVARCHAR(50),
        order_date NVARCHAR(50),
        shipping_date NVARCHAR(50),
        due_date NVARCHAR(50),
        sale_amount NVARCHAR(50),
        quantity NVARCHAR(50)
    )
    """)

# Closing the Connection
cursor.close()
conn.close()