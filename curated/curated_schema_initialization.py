import pyodbc

# Connecting to the Database
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;"
    , autocommit=True
)

cursor = conn.cursor()

# 1. Creating the Schema
cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'curated') EXEC('CREATE SCHEMA curated')")
print("Schema 'curated' ensured.")

# 2. Creating the Dimension Tables
## dim_customers
cursor.execute("""
    CREATE TABLE curated.dim_customers (
        customer_key INT PRIMARY KEY,
        customer_id INT,
        customer_number NVARCHAR(10),
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        country VARCHAR(50),
        marital_status VARCHAR(7),
        gender VARCHAR(6),
        birthdate DATE,
        create_date DATE
    )
    """)

## dim_products
cursor.execute("""
    CREATE TABLE curated.dim_products (
        product_key INT PRIMARY KEY,
        product_number NVARCHAR(15),
        product_name NVARCHAR(100),
        category_id NVARCHAR(5),
        category VARCHAR(50),
        subcategory VARCHAR(75),
        maintenance BIT,
        cost INT,
        product_line VARCHAR(8),
        start_date DATE,
        end_date DATE
    )
    """)

# 3. Creating the Fact Table
## fact_sales
cursor.execute("""
    CREATE TABLE curated.fact_sales (
        sales_key INT PRIMARY KEY,
        product_key INT,
        customer_key INT,
        order_number NVARCHAR(10),
        order_date DATE,
        shipping_date DATE,
        due_date DATE,
        sales_amount INT,
        quantity INT,
        price INT
    )
    """)

# Sanity Check
print("Curated Schema and Tables Initiated Successfully!")

# Closing the Connection
cursor.close()
conn.close()