import sys
import os

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    project_root = os.getcwd()
if project_root not in sys.path: sys.path.append(project_root)

from pyspark.sql.functions import col, sum as _sum, avg

from config.spark_settings import sparkSessionBuilder
from config.paths import DataPaths

spark = sparkSessionBuilder()

def check_table(table_name, numeric_col=None, date_col=None):
    print(f"\n🔎 --- AUDITING: {table_name.upper()} ---")
    
    try:
        bronze_df = spark.read.format("delta").load(os.path.join(DataPaths.BRONZE, table_name))
        silver_df = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, table_name))
    except Exception as e:
        print(f"Could not load table {table_name} Error: {e}")
        return

    b_count = bronze_df.count()
    s_count = silver_df.count()
    dropped = b_count - s_count
    
    print(f"Count: Bronze={b_count} | Silver={s_count}")
    if dropped > 0:
        print(f"Deduplicated: {dropped} rows removed.")
    elif dropped == 0:
        print(f"No duplicates found (Counts match).")
    else:
        print(f"Silver has more rows")

    print("structural Check:")
    
    # Check a number column if exists
    if numeric_col:
        dtype = [f.dataType for f in silver_df.schema.fields if f.name == numeric_col][0]
        print(f"Column '{numeric_col}' is type: {dtype}")
        
        # math to check if its a number >>>>>> _sum = sum from spark, not python math
        total = silver_df.select(_sum(col(numeric_col))).collect()[0][0]
        print(f"(Math Test: Sum of {numeric_col} = {total:,.2f})")

    # Check Date Column
    if date_col:
        dtype = [f.dataType for f in silver_df.schema.fields if f.name == date_col][0]
        print(f"Column '{date_col}' is type: {dtype}")

    print("Sample Data:")
    silver_df.select(
        [col for col in silver_df.columns if col in [numeric_col, date_col, 'order_id', 'customer_id']][:3]
    ).show(2, truncate=False)


spark = sparkSessionBuilder()

def check_gold():
    print("\nGOLD LAYER")
    
    # Check Fact Sales
    try:
        df_sales = spark.read.format("delta").load(os.path.join(DataPaths.GOLD, "fact_sales"))
        row_count = df_sales.count()
        print(f"💰 fact_sales: {row_count} rows")
        
        # Average Delivery Time
        # Date Diff calculation worked
        avg_days = df_sales.select(avg("delivery_days")).collect()[0][0]
        print(f"   🚚 Avg Delivery Time: {avg_days:.2f} days")
        
        # keys match
        # check 'customer_id' exists in Dims
        df_cust = spark.read.format("delta").load(os.path.join(DataPaths.GOLD, "dim_customers"))
        print(f"👥 dim_customers: {df_cust.count()} rows")
        
        # integrity check
        # "How many sales have a customer that we DON'T know?" (Should be 0)
        orphans = df_sales.join(df_cust, "customer_id", "left_anti").count()
        if orphans == 0:
            print("Integrity Check Passed: All sales linked to valid customers.")
        else:
            print(f"Integrity Warning: {orphans} sales have missing customer details.")
            
    except Exception as e:
        print(f"XXXX Error reading Gold: {e}")

def check():
    # Read Bronze Reviews
    df_bronze = spark.read.format("delta").load("/Volumes/workspace/default/olist-ecommerce-lakehouse/lakehouse/bronze/reviews")

    # Show me what is inside 'review_score'
    df_bronze.select("review_score", "review_creation_date").show(5, truncate=False)

if __name__ == "__main__":
    #check gold layer
    #check_gold()

    check()
    # Check Orders (Dates)
    #check_table("orders", date_col="order_purchase_timestamp")
    
    # Check Items (Prices)
    #check_table("items", numeric_col="price")