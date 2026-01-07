import os

class DataPaths:
    IS_DATABRICKS = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

    if IS_DATABRICKS: 
      # databricks
      BASE_DIR = "/Volumes/workspace/default/olist-ecommerce-lakehouse"
    else: 
      # local
      BASE_DIR = os.getcwd()

    RAW = os.path.join(BASE_DIR, "data", "raw")
    BRONZE = os.path.join(BASE_DIR, "data", "lakehouse", "bronze")
    SILVER = os.path.join(BASE_DIR, "data", "lakehouse", "silver")
    GOLD = os.path.join(BASE_DIR, "data", "lakehouse", "gold")
