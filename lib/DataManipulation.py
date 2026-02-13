from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# ---------------- Filter Closed Orders ----------------
def filter_closed_orders(orders_df: DataFrame) -> DataFrame:
    """
    Filters orders with status 'CLOSED'
    """
    return orders_df.filter(col("order_status") == "CLOSED")


# ---------------- Join Orders and Customers ----------------
def join_orders_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    Joins orders and customers DataFrames on 'customer_id'
    """
    return orders_df.join(customers_df, on="customer_id", how="inner")


# ---------------- Count Orders by State ----------------
def count_orders_state(joined_df: DataFrame) -> DataFrame:
    """
    Aggregates number of orders by state
    """
    return joined_df.groupBy("state").count()
