import schedule
import time
import subprocess
import os

# Define the paths to your transformation scripts based on your repo structure
BASE_DIR = "transformation"
INGESTION_DIR = "ingestion"

def run_task(script_name, directory):
    """Helper to run the python scripts in the repository."""
    script_path = os.path.join(directory, script_name)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Running: {script_name}...")
    try:
        # Using subprocess to run scripts as standalone processes
        subprocess.run(["python", script_path], check=True)
        print(f"Successfully completed {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")

def run_ingestion():
    """Initial step: Push CSV data to the ingestion schema."""
    run_task("ingestion_push.py", INGESTION_DIR)

def run_customer_transformations():
    """Runs transformations for both ERP and CRM customer data."""
    print("Starting Customer Transformation Jobs...")
    run_task("cleaning_erp_customer.py", BASE_DIR)
    run_task("cleaning_crm_costumer.py", BASE_DIR)

def run_location_transformations():
    """Runs transformations for ERP location data."""
    print("Starting Location Transformation Jobs...")
    run_task("cleaning_erp_location.py", BASE_DIR)

def run_product_transformations():
    """Runs transformations for Product and Sales data."""
    print("Starting Product and Sales Transformation Jobs...")
    run_task("cleaning_erp_product_category.py", BASE_DIR)
    run_task("cleaning_crm_product.py", BASE_DIR)
    run_task("cleaning_crm_sales.py", BASE_DIR)

# --- EXECUTION SCHEDULE ---

# 1. Run ingestion first to ensure fresh data is available
run_ingestion()

# 2. Run initial transformations immediately
run_customer_transformations()
run_location_transformations()
run_product_transformations()

# 3. Schedule recurring runs
# Customers updated every hour
schedule.every().hour.do(run_customer_transformations)

# Locations and Products updated daily
schedule.every().day.at("02:00").do(run_location_transformations)
schedule.every().day.at("03:00").do(run_product_transformations)

# Keep ingestion synced (e.g., every 6 hours)
schedule.every(6).hours.do(run_ingestion)

print("ETL Scheduler started and running...")

while True:
    schedule.run_pending()
    time.sleep(60) # Check every minute