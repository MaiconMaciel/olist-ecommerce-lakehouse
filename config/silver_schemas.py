from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType, DecimalType

# 1. Customers
silver_customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

# 2. Geolocation
silver_geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", DoubleType(), True), 
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True)
])

# 3. Order Items
silver_order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), True), 
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True), 
    StructField("price", FloatType(), True),                   
    StructField("freight_value", FloatType(), True)            
])

# 4. Order Payments
silver_order_payments_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", FloatType(), True) 
])

# 5. Order Reviews
silver_order_reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True)
])

# 6. Orders
silver_orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True)
])

# 7. Products
silver_products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True)
])

# 8. Sellers
silver_sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

# 9. Product Category Translation
silver_product_category_name_translation_schema = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), False)
])

SILVER_TABLES_CONFIG = [
    {
        "table_name": "orders",          # Target Name (in Silver)
        "source_table": "orders",        # Source Name (from Bronze)
        "schema": silver_orders_schema,  # The Strict Schema (Timestamps)
        "keys": ["order_id"]             # Unique Keys for Deduplication
    },
    {
        "table_name": "customers",
        "source_table": "customers",
        "schema": silver_customers_schema,
        "keys": ["customer_id"]
    },
    {
        "table_name": "items",
        "source_table": "items",
        "schema": silver_order_items_schema,
        "keys": ["order_id", "order_item_id"] # !
    },
    
    {
        "table_name": "products",
        "source_table": "products",
        "schema": silver_products_schema,
        "keys": ["product_id"]
    },
    {
        "table_name": "sellers",
        "source_table": "sellers",
        "schema": silver_sellers_schema,
        "keys": ["seller_id"]
    },
    {
        "table_name": "reviews",
        "source_table": "reviews",
        "schema": silver_order_reviews_schema,
        "keys": ["review_id"]
    },
    {
        "table_name": "payments",
        "source_table": "payments",
        "schema": silver_order_payments_schema,
        "keys": ["order_id", "payment_sequential"]
    },
    {
        "table_name": "geolocation",
        "source_table": "geolocation",
        "schema": silver_geolocation_schema,
        "keys": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"] 
        # geolocation has no matching results, this is the best way i could think to handle them
    },
    {
        "table_name": "category_translations",
        "source_table": "category_translations",
        "schema": silver_product_category_name_translation_schema,
        "keys": ["product_category_name"]
    }
]