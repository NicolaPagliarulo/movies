import psutil
from pyspark.sql import SparkSession


def create_spark_session(app_name="app"):
    """
    Creates a SparkSession instance.

    :param app_name: The name of the Spark application.
    :return: SparkSession
    """
    physical_cores = psutil.cpu_count(logical=False)
    spark = SparkSession.builder.appName(app_name).config("spark.default.parallelism", str(physical_cores)).getOrCreate()
    return spark
