import os
from pyspark.sql import SparkSession
from delta.tables import *

def sparkSessionBuilder():

    is_databricks = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

    if is_databricks:
        # Get databricks session
        return SparkSession.builder.getOrCreate()

    else:
        # local development
        return (
            SparkSession.builder
            .appName("OlistLakehouse")
            .master("local[*]")  # Use all cores
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")  # Enable Delta
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
