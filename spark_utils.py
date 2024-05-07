from pyspark.sql import SparkSession


def create_spark_session(app_name="app"):
    """
    Creates a SparkSession instance.

    :param app_name: The name of the Spark application
    :return: SparkSession
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
