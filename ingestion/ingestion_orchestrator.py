import subprocess
import sys

# Define your files in the exact order they should run
scripts = [
    "ingestion/ingestion_schema_initiliazation.py",
    "ingestion/ingestion_push.py",
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