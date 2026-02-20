from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config

def get_spark_session(env: str) -> SparkSession:
    """
    Returns a SparkSession based on the environment.

    :param env: "LOCAL" or "PROD"
    :return: SparkSession
    """
    if env.upper() == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .config(
                'spark.driver.extraJavaOptions',
                '-Dlog4j.configuration=file:log4j.properties'
            ) \
            .master("local[2]") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .enableHiveSupport() \
            .getOrCreate()
