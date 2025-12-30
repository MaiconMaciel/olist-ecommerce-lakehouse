import os
import sys

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    project_root = os.getcwd()
if project_root not in sys.path: sys.path.append(project_root)

from config.spark_settings import sparkSessionBuilder

from config.paths import DataPaths
from config.silver_schemas import SILVER_TABLES_CONFIG

# helper function
from utils import clean_and_enforce_schema

spark = sparkSessionBuilder()

table_config = SILVER_TABLES_CONFIG

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

spark = sparkSessionBuilder()

def run_silver_layer():
    print("Starting Silver Layer")
    
    # ONE LOOP TO RULE THEM ALL
    for table_config in SILVER_TABLES_CONFIG:
        table_name = table_config["table_name"]
        source_name = table_config["source_table"]
        target_schema = table_config["schema"]
        dedup_keys = table_config["keys"]
        
        print(f"🔨 Processing: {table_name}")
        
        try:
            # Read bronze
            input_path = os.path.join(DataPaths.BRONZE, source_name)
            df = spark.read.format("delta").load(input_path)
            
            # CLEAN & ENFORCE SCHEMA
            # Trims strings, Fixes Dates, Casts Floats
            df_clean = clean_and_enforce_schema(df, target_schema)
            
            # SPECIFIC BUSINESS RULES
            # Only strictly necessary fixes go here
            if table_name == "orders":
                # Example: If status is null, set to 'created'
                df_clean = df_clean.fillna({"order_status": "created"})
            
            # DEDUPLICATE for all
            df_final = df_clean.dropDuplicates(dedup_keys)
            
            # WRITE (Silver)
            output_path = os.path.join(DataPaths.SILVER, table_name)
            df_final.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(output_path)
                
            print(f"Success: {table_name}")
            
        except Exception as e:
            print(f"XXXX Error processing {table_name}: {e}")

if __name__ == "__main__":
    run_silver_layer()