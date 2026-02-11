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
        .master("local[2]") \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.ui.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark