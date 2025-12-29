import sys
import os
from pyspark.sql.functions import col, first, datediff

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_settings import sparkSessionBuilder
from config.paths import DataPaths

spark = sparkSessionBuilder()

def create_dim_products():
    print("🔨 Building Gold: dim_products")
    
    # Read Silver Tables
    df_products = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "products"))
    df_trans = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "category_translations"))
    
    # Join (Enrichment)
    # We want ALL products, even if they don't have a translation (Left Join)
    df_joined = df_products.join(
        df_trans,
        on="product_category_name",
        how="left"
    )
    
    # Select & Rename for Business Clarity
    # In Gold, we use friendly names for Analysts
    df_final = df_joined.select(
        col("product_id"),
        col("product_category_name").alias("category_pt"),       # Portuguese
        col("product_category_name_english").alias("category_en"), # English
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm")
    )
    
    #Save
    path = os.path.join(DataPaths.GOLD, "dim_products")
    df_final.write.format("delta").mode("overwrite").save(path)
    print(f"dim_products saved! Rows: {df_final.count()}")


def create_dim_customers():
    print("Building Gold: dim_customers")
    
    #Read Silver Tables
    df_cust = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "customers"))
    df_geo = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "geolocation"))
    
    # PREPARE GEOLOCATION (Critical Deduplication Step)
    # The geolocation table has many points per zip code. We need just ONE per zip.
    # logic: Group by Zip, take the FIRST city/state/lat/lng found.
    df_geo_unique = df_geo.groupBy("geolocation_zip_code_prefix").agg(
        first("geolocation_lat").alias("lat"),
        first("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city_geo"),
        first("geolocation_state").alias("state_geo")
    )
    
    #Join on Zip Code
    df_joined = df_cust.join(
        df_geo_unique,
        df_cust.customer_zip_code_prefix == df_geo_unique.geolocation_zip_code_prefix,
        how="left"
    )
    
    #Select Final Columns
    df_final = df_joined.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_city").alias("city"),
        col("customer_state").alias("state"),
        col("lat"),
        col("lng")
    )
    
    #Save
    path = os.path.join(DataPaths.GOLD, "dim_customers")
    df_final.write.format("delta").mode("overwrite").save(path)
    print(f"dim_customers saved! Rows: {df_final.count()}")

def create_fact_sales():
    print("Building Gold: fact_sales")
    
    # Read Silver Tables
    df_orders = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "orders"))
    df_items = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "items"))
    
    # Join Items with Orders
    # We use Inner Join because we only want sold items that have a valid order header
    df_joined = df_items.join(df_orders, on="order_id", how="inner")
    
    # Calculate Metrics (KPIs)
    # KPI: How long did it actually take to deliver?
    df_enriched = df_joined.withColumn(
        "delivery_days", 
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )
    
    # Select Final Columns (Star Schema Keys + Metrics)
    df_final = df_enriched.select(
        # Keys (Foreign Keys to Dims)
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("seller_id"),
        
        # Time Dimensions (For Trend Analysis)
        col("order_purchase_timestamp").alias("purchase_date"),
        col("order_delivered_customer_date").alias("delivered_date"),
        
        # Metrics (Facts)
        col("price"),
        col("freight_value"),
        col("delivery_days"),
        col("order_status")
    )
    
    # Save
    path = os.path.join(DataPaths.GOLD, "fact_sales")
    df_final.write.format("delta").mode("overwrite").save(path)
    print(f"fact_sales saved! Rows: {df_final.count()}")

def create_fact_reviews():
    print("Building Gold: fact_reviews")
    
    # Read Silver
    df_reviews = spark.read.format("delta").load(os.path.join(DataPaths.SILVER, "reviews"))
    
    # Select (Keep it simple for now)
    # We will join this with Products later in the specific AI/RAG script
    df_final = df_reviews.select(
        col("review_id"),
        col("order_id"),
        col("review_score"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("review_creation_date")
    )
    #SAve
    path = os.path.join(DataPaths.GOLD, "fact_reviews")
    df_final.write.format("delta").mode("overwrite").save(path)
    print(f"fact_reviews saved! Rows: {df_final.count()}")

if __name__ == "__main__":
    # Run the Dims (Nouns)
    create_dim_products()
    create_dim_customers()
    
    # Run the Facts (Verbs)
    create_fact_sales()
    create_fact_reviews()