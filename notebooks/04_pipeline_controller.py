import os
import sys
import subprocess
import time

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    project_root = os.getcwd()

print(f"STARTING PIPELINE")
print(f"Environment: {'DATABRICKS' if os.getenv('DATABRICKS_RUNTIME_VERSION') else 'LOCAL DOCKER'}")

def run_step(step_name, script_path, db_notebook_name):
    """
    Hybrid Orchestration:
    Databricks: Uses dbutils.notebook.run()
    Local: Uses subprocess to run python file
    """
    print(f"\n⏳ Starting Step: {step_name}")
    start_time = time.time()
    
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        # Databricks
        try:
            dbutils.notebook.run(db_notebook_name, 0)
            status = "SUCCESS"
        except Exception as e:
            print(f"Error in {step_name}: {e}")
            sys.exit(1)
            
    else:
        # Local
        full_path = os.path.join(project_root, script_path)
        result = subprocess.run(["python", full_path], capture_output=True, text=True)
        
        if result.returncode == 0:
            status = "SUCCESS"
        else:
            print(f"Error in {step_name}:\n{result.stderr}")
            sys.exit(1)

    duration = time.time() - start_time
    print(f"{step_name} Completed in {duration:.2f} seconds.")

# Executions

# Bronze
run_step("1. Ingestion (Bronze)", "notebooks/01_ingest_bronze.py", "01_ingest_bronze")

# Silver
run_step("2. Transformation (Silver)", "notebooks/02_transform_silver.py", "02_transform_silver")

# Gold
run_step("3. Modeling (Gold)", "notebooks/03_transform_gold.py", "03_transform_gold")

print("Orchestration Finished")
