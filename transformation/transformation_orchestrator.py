import subprocess
import sys

# Define your files in the exact order they should run
scripts = [
    "transformation/transformation_schema_initiliazation.py",
    "transformation/cleaning_crm_costumer.py",
    "transformation/cleaning_crm_product.py",
    "transformation/cleaning_crm_product.py"
    "transformation/cleaning_erp_customer.py",
    "transformation/cleaning_erp_location.py",
    "transformation/cleaning_erp_product_category.py",
]

def run_scripts():
    for script in scripts:
        print(f"--- Starting: {script} ---")
        try:
            # shell=True is often needed on Windows for path resolution
            result = subprocess.run([sys.executable, script], check=True)
            print(f"--- Finished: {script} successfully ---\n")
        except subprocess.CalledProcessError as e:
            print(f"ERROR: {script} failed. Stopping the pipeline.")
            break

if __name__ == "__main__":
    run_scripts()