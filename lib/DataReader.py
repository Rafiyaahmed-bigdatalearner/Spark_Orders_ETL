from pyspark.sql import SparkSession
from lib import ConfigReader

# ---------------- Customers ----------------
def get_customers_schema():
    """
    Defines schema for customers CSV
    """
    schema = (
        "customer_id INT,"
        "customer_fname STRING,"
        "customer_lname STRING,"
        "username STRING,"
        "password STRING,"
        "address STRING,"
        "city STRING,"
        "state STRING,"
        "pincode STRING"
    )
    return schema

def read_customers(spark: SparkSession, env: str):
    """
    Reads customers CSV into a Spark DataFrame
    """
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

# ---------------- Orders ----------------
def get_orders_schema():
    """
    Defines schema for orders CSV
    """
    schema = (
        "order_id INT,"
        "order_date STRING,"
        "customer_id INT,"
        "order_status STRING"
    )
    return schema

def read_orders(spark: SparkSession, env: str):
    """
    Reads orders CSV into a Spark DataFrame
    """
    conf = ConfigReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)
