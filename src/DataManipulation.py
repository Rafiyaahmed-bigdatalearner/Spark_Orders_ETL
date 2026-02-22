"""
DataManipulation.py

Reusable PySpark functions for:
- Filtering orders
- Joining orders with customers
- Counting orders by state
- Applying Slowly Changing Dimension Type 2 (SCD2)

Designed for a Bronze → Silver → Gold medallion pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_date

# ---------------- Filter Closed Orders ----------------
def filter_closed_orders(orders_df: DataFrame) -> DataFrame:
    """Keep only orders with status 'CLOSED'."""
    return orders_df.filter(col("order_status") == "CLOSED")

# ---------------- Join Orders and Customers ----------------
def join_orders_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """Join orders with current customers (current_flag=1)."""
    return orders_df.join(customers_df.filter(col("current_flag") == 1), on="customer_id", how="inner")

# ---------------- Count Orders by State ----------------
def count_orders_state(joined_df: DataFrame) -> DataFrame:
    """Aggregate number of orders by state."""
    return joined_df.groupBy("state").count()

# ---------------- Apply Simple SCD2 ----------------
def apply_scd2(new_df: DataFrame, existing_df: DataFrame) -> DataFrame:
    """
    Simple Slowly Changing Dimension Type 2 for customer dimension.
    - Marks old records inactive
    - Adds new records as active
    """
    # Detect changed records
    changed = new_df.alias("new").join(
        existing_df.alias("old"),
        on="customer_id",
        how="left"
    ).filter(col("new.state") != col("old.state"))

    # Old records set inactive
    old_inactive = existing_df.join(
        changed.select("customer_id"),
        on="customer_id",
        how="inner"
    ).withColumn("current_flag", lit(0)).withColumn("end_date", current_date())

    # New records active
    new_records = changed.select("new.*").withColumn("current_flag", lit(1)).withColumn("start_date", current_date()).withColumn("end_date", lit(None))

    # Keep unchanged old records
    unchanged = existing_df.join(
        changed.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )

    return unchanged.unionByName(old_inactive).unionByName(new_records)
