import pyodbc
import csv
import os

# Connecting to the Databse
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=dedg_database;"
    "Trusted_Connection=yes;"
    , autocommit=True
)

cursor = conn.cursor()

# Preparing the Push
file_map = {
    "CUST_AZ12.csv": "erp_customer",
    "LOC_A101.csv": "erp_location",
    "PX_CAT_G1V2.csv": "erp_product_category",
    "cust_info.csv": "crm_customer",
    "prd_info.csv": "crm_product",
    "sales_details.csv": "crm_sales"
}

folder_path = r"C:\Users\User\Desktop\datasets"

# Pushing
for filename, table_name in file_map.items():
        file_path = os.path.join(folder_path, filename)
        
        if not os.path.exists(file_path):
            print(f"Skipping {filename}: File not found in folder.") # Sanity Check
            continue

        #Cleans Old Data (Assuming we get the entirety every time)
        cursor.execute(f"TRUNCATE TABLE ingestion.{table_name}")
        print(f"Cleaned table ingestion.{table_name}")

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip the header
            
            # Prepare the SQL: INSERT INTO ingestion.table (col1, col2) VALUES (?, ?)
            # The number of ? must match the number of columns in the CSV header
            placeholders = ", ".join(["?" for _ in header])
            sql = f"INSERT INTO ingestion.{table_name} VALUES ({placeholders})"
            
            print(f"Ingesting {filename} into ingestion.{table_name}...")
            
            # Insert rows one by one
            for row in reader:
                # row[:len(header)] ensures we don't grab extra empty columns if the CSV is messy
                cursor.execute(sql, row[:len(header)])

        print(f"Ingesting completed!")



# Closing the Connection
cursor.close()
conn.close()