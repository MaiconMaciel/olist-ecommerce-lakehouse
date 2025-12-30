import os
from pyspark.sql import SparkSession
from delta.tables import *

def sparkSessionBuilder():

    is_databricks = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

  # databricks
    if is_databricks:
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.ansi.enabled", "false")
        return spark

    else:
        # local
        return (
            SparkSession.builder
            .appName("OlistLakehouse")
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.ansi.enabled", "false") 
            .getOrCreate()
        )
