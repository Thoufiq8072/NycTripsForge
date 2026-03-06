from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session(app_name: str = "nyc_taxi_pipeline") -> SparkSession:
    """
    Create and configure a SparkSession with Delta Lake support.

    :param app_name: The name of the Spark application.
    :return: A configured SparkSession instance.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark