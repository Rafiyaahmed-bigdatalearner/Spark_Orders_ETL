from pyspark.sql import SparkSession
from logger import Log4j
from DataManipulation import (
    filter_closed_orders, 
    join_orders_customers, 
    count_orders_state, 
    apply_scd2
)

# ---------------- Spark Session ----------------
spark = SparkSession.builder \
    .appName("Retail Analysis Pipeline") \
    .getOrCreate()

# ---------------- Logger ----------------
logger = Log4j(spark, app_name="retail_analysis")
logger.info("Spark session started.")

# ---------------- Bronze Layer: Raw Data ----------------
orders_raw_df = spark.read.csv("bronze/orders.csv", header=True, inferSchema=True)
customers_raw_df = spark.read.csv("bronze/customers.csv", header=True, inferSchema=True)
logger.info("Raw data loaded successfully.")

# ---------------- Silver Layer: Cleaned / SCD2 ----------------
orders_closed_df = filter_closed_orders(orders_raw_df)
logger.info(f"Filtered closed orders: {orders_closed_df.count()} rows.")

# Assume existing customer dimension exists in silver
customers_existing_df = spark.read.parquet("silver/customers_silver.parquet")
customers_silver_df = apply_scd2(customers_raw_df, customers_existing_df)
logger.info("Applied SCD2 to customers dimension.")

# ---------------- Gold Layer: Aggregations ----------------
orders_customers_df = join_orders_customers(orders_closed_df, customers_silver_df)
orders_by_state_df = count_orders_state(orders_customers_df)
orders_by_state_df.write.mode("overwrite").parquet("gold/orders_by_state.parquet")
logger.info("Orders by state written to Gold layer successfully.")

spark.stop()
logger.info("Spark session stopped.")
