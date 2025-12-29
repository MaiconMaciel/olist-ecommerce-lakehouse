from pyspark.sql.types import StructType, StructField, StringType

#schema for all raw source files
customers_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_unique_id", StringType(), nullable=False),
    StructField("customer_zip_code_prefix", StringType(), nullable=True),
    StructField("customer_city", StringType(), nullable=True),
    StructField("customer_state", StringType(), nullable=True),
])

geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), False),
    StructField("geolocation_lat", StringType(), False),
    StructField("geolocation_lng", StringType(), False),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("freight_value", StringType(), True),
])

order_payments_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", StringType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
])

order_reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", StringType(), False),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", StringType(), True),
    StructField("product_description_lenght", StringType(), True),
    StructField("product_photos_qty", StringType(), True),
    StructField("product_weight_g", StringType(), True),
    StructField("product_length_cm", StringType(), True),
    StructField("product_height_cm", StringType(), True),
    StructField("product_width_cm", StringType(), True),
])

sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

product_category_name_translation_schema = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), False),
])

#global configs for each table
TABLES_CONFIG = [
    {
        "table_name": "orders", # clearer name
        "schema": orders_schema, # calls a schema defined above
        "source_file": "olist_orders_dataset.csv" # source file name
    },
    {
        "table_name": "items",
        "schema": order_items_schema,
        "source_file": "olist_order_items_dataset.csv"
    },
    {
        "table_name": "customers",
        "schema": customers_schema,
        "source_file": "olist_customers_dataset.csv"
    },
    {
        "table_name": "products",
        "schema": products_schema,
        "source_file": "olist_products_dataset.csv"
    },
    {
        "table_name": "sellers",
        "schema": sellers_schema,
        "source_file": "olist_sellers_dataset.csv"
    },
    {
        "table_name": "reviews",
        "schema": order_reviews_schema,
        "source_file": "olist_order_reviews_dataset.csv"
    },
    {
        "table_name": "payments",
        "schema": order_payments_schema,
        "source_file": "olist_order_payments_dataset.csv"
    },
    {
        "table_name": "geolocation",
        "schema": geolocation_schema,
        "source_file": "olist_geolocation_dataset.csv"
    },
    {
        "table_name": "category_translations",
        "schema": product_category_name_translation_schema,
        "source_file": "product_category_name_translation.csv"
    }
]