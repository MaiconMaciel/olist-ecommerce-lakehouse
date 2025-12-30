import os
import sys

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    project_root = os.getcwd()
if project_root not in sys.path: sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# check use later
from delta import configure_spark_with_delta_pip
from delta.tables import *

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from config.spark_settings import sparkSessionBuilder

from config.schemas import TABLES_CONFIG
from config.paths import DataPaths

# calls spark configs from config/spark_settings.py
spark = sparkSessionBuilder()

# ingestion for bronze layer, key feature KEEP DATA SAFE
def ingest_table(TABLES_CONFIG):
    table_name = TABLES_CONFIG["table_name"]
    source_file = TABLES_CONFIG["source_file"]
    schema = TABLES_CONFIG["schema"]
    
    print(f"Starting ingestion for: {source_file}.")

    input_path = os.path.join(DataPaths.RAW, source_file)
    output_path = os.path.join(DataPaths.BRONZE, table_name)

    try:
        print('start')
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .schema(schema) \
            .load(input_path)

        # metadata
        df_bronze = df \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("source_file", lit(source_file))

        # Write delta
        df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)
        
        print(f"Saved {table_name} to {output_path}")

    except Exception as e:
        print(f"Error processing {table_name}: {e}")

if __name__ == "__main__":
    for table_conf in TABLES_CONFIG:
        ingest_table(table_conf)