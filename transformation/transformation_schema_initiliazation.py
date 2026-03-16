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
cursor.execute("CREATE SCHEMA transformation")

# Creating the Tables
## erp_customer table (CUST_AZ12.csv)
cursor.execute("""
    CREATE TABLE transformation.erp_customer (
        ID NVARCHAR(15),
        date_of_birth DATE,
        gender VARCHAR(6)
    )
    """)

## erp_location table (LOC_A101.csv)
cursor.execute("""
    CREATE TABLE transformation.erp_location (
        ID NVARCHAR(10),
        country VARCHAR(50)
    )
    """)

## erp_product_category table (PX_CAT_G1V2.csv)
cursor.execute("""
    CREATE TABLE transformation.erp_product_category (
        ID NVARCHAR(5),
        category VARCHAR(50),
        subcategory VARCHAR(75),
        requires_maintence BIT
    )
    """)

## crm_customer table (cust_info.csv)
cursor.execute("""
    CREATE TABLE transformation.crm_customer (
        ID INT(5),
        customer_key NVARCHAR(10),
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        marital_status VARCHAR(7),
        gender VARCHAR(6),
        date_of_creation DATE
    )
    """)

## crm_product table (prd_info.csv)
cursor.execute("""
    CREATE TABLE transformation.crm_product (
        ID NVARCHAR(5),
        product_key NVARCHAR(15),
        product_category NVARCHAR(5),
        product_name NVARCHAR(100),      
        product_cost INT,
        product_line VARCHAR(8),
        product_start_date DATE,
        product_end_date DATE
    )
    """)

## crm_sales table (sales_details.csv)
cursor.execute("""
    CREATE TABLE transformation.crm_sales (
        ID NVARCHAR(10),
        product_key NVARCHAR(10),
        customer_id INT,
        order_date DATE,
        shipping_date DATE,
        due_date DATE,
        sale_amount INT,
        quantity INT,
        price INT
    )
    """)

#Sanity Check 
print("Schema Initiated Succefully!")

# Closing the Connection
cursor.close()
conn.close()