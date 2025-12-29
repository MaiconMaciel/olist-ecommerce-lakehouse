import os

class DataPaths:
    IS_DATABRICKS = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

    if IS_DATABRICKS:
      # future add databricks path using same structure
      print("Databricks, you need to add the paths to config")
      pass
    else:
      BASE_DIR = os.getcwd()
      RAW = os.path.join(BASE_DIR, "data", "raw")
      BRONZE = os.path.join(BASE_DIR, "data", "lakehouse", "bronze")
      SILVER = os.path.join(BASE_DIR, "data", "lakehouse", "silver")
      GOLD = os.path.join(BASE_DIR, "data", "lakehouse", "gold")
