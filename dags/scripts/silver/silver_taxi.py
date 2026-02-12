from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from scripts.spark.spark_session import create_spark_session

spark = create_spark_session("yellow_taxi_silver_loader")    

BASE_PATH = "/opt/airflow/data"
BRONZE_PATH = f"{BASE_PATH}/bronze/trips_data"
SILVER_PATH = f"{BASE_PATH}/silver/trips_data"

def load_data(file_path: str):
    """
    Load data from a specified file path and type into a Spark DataFrame.

    :param file_path: The path to the data file.
    :return: A Spark DataFrame containing the loaded data.
    """
    df = spark.read.format("delta").load(file_path)

    return df

def schema_enforcement(df):
    """
    Enforce the expected schema on a Spark DataFrame.

    :param df: The Spark DataFrame to be transformed.
    :return: A Spark DataFrame with the enforced schema.
    """
    df = df.select(
        col("VendorID").cast(StringType()).alias("vendor_id"),
        col("tpep_pickup_datetime").cast(TimestampType()).alias("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime").cast(TimestampType()).alias("tpep_dropoff_datetime"),
        col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
        col("PULocationID").cast(IntegerType()).alias("pu_location_id"),
        col("DOLocationID").cast(IntegerType()).alias("do_location_id"),
        col("payment_type").cast(IntegerType()).alias("payment_type"),
        col("fare_amount").cast(DoubleType()).alias("fare_amount"),
        col("extra").cast(DoubleType()).alias("extra"),
        col("mta_tax").cast(DoubleType()).alias("mta_tax"),
        col("tip_amount").cast(DoubleType()).alias("tip_amount"),
        col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
        col("improvement_surcharge").cast(DoubleType()).alias("improvement_surcharge"),
        col("total_amount").cast(DoubleType()).alias("total_amount"),
        col("congestion_surcharge").cast(DoubleType()).alias("congestion_surcharge"),
        col("Airport_fee").cast(DoubleType()).alias("airport_fee"),
        col("cbd_congestion_fee").cast(DoubleType()).alias("cbd_congestion_fee"),
        col("_ingested_date"),
        col("_data_source"),
        col("_load_timestamp")
    )
    return df

def transform_data(df):
    """
    Perform data transformations on the input DataFrame.

    :param df: The input Spark DataFrame to be transformed.
    :return: A transformed Spark DataFrame.
    """
    # Remove duplicate records based on a combination of key columns
    df = df.dropDuplicates(["vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pu_location_id", "do_location_id"])

    # Filter out records with invalid or inconsistent data
    df = df.filter((col("vendor_id").isNotNull()) & ((col("tpep_pickup_datetime")) < (col("tpep_dropoff_datetime"))) & (col("trip_distance") > 0) & (col("fare_amount") >= 0) & (col("total_amount") >= 0))

    # Further filter out records with null values in critical columns
    df = df.filter('(vendor_id IS NOT NULL) AND (tpep_pickup_datetime IS NOT NULL) AND (tpep_dropoff_datetime IS NOT NULL) AND (pu_location_id IS NOT NULL) AND (do_location_id IS NOT NULL)')
  
    # Null Handling: Fill null values in non-critical columns with default values
    df = df.fillna({
        "extra": 0.0,
        "mta_tax": 0.0,
        "tip_amount": 0.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.0,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "cbd_congestion_fee": 0.0
    })

    df = df.withColumn("trip_duration", (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60) \
        .withColumn("trip_date", to_date(col("tpep_pickup_datetime"))) 

    df = df.select(
        "vendor_id",
        "trip_date",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_duration",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "_ingested_date",
        "_data_source",
        "_load_timestamp"
    )

    return df

def save_data(df, file_path: str):
    """
    Save a Spark DataFrame to a specified file path in Delta format.

    :param df: The Spark DataFrame to be saved.
    :param file_path: The path where the DataFrame should be saved.
    """
    try:
        df.write.format("delta").mode("overwrite").save(file_path)
    except Exception as e:
        print(f"Error saving data: {e}")

def main():
    try:
        df = load_data(BRONZE_PATH)
        df = schema_enforcement(df)
        df = transform_data(df)
    except Exception as e:
        print(f"Error loading data: {e}")
        return  # Stop execution if data loading fails
    else:
        save_data(df, SILVER_PATH)
        print("Data Stored completed successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
    
    