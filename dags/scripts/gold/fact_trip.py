from numpy import broadcast
from pyparsing import col
from dags.scripts.gold.dim_location import DIM_LOCATION_PATH
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from scripts.spark.spark_session import create_spark_session

spark = create_spark_session("yellow_taxi_silver_loader")    

BASE_PATH = "/opt/airflow/data"
SILVER_PATH = f"{BASE_PATH}/silver/trips_data"
DIM_LOCATION_PATH = f"{BASE_PATH}/gold/dim_location"
FACT_TRIP_PATH = f"{BASE_PATH}/gold/fact_trip"

def load_data(file_path: str):
    """
    Load data from a specified file path and type into a Spark DataFrame.

    :param file_path: The path to the data file.
    :return: A Spark DataFrame containing the loaded data.
    """
    df = spark.read.format("delta").load(file_path)

    return df

def transform_data(df):
    """
    Transform the input DataFrame by adding a surrogate key column.

    :param df: The input Spark DataFrame to be transformed.
    :return: A transformed Spark DataFrame with an added surrogate key column.
    """
    # Remove audit columns that are not needed in the fact table
    df = df.drop('_ingested_date', '_data_source', '_load_timestamp')

    # Load the dimension table to get the surrogate keys for pickup and dropoff locations
    df_location = load_data(DIM_LOCATION_PATH).select("location_id", "location_sk")

    # Join the fact table with the dimension table to get the surrogate keys for pickup and dropoff locations
    df = df.join(
        broadcast(df_location).alias("pu"), 
        df["pu_location_id"] == col("pu.location_id"), 
        "left"
    ) \
    .select(df["*"], col("pu.location_sk").alias("pu_location_sk")) \
    .join(
        broadcast(df_location).alias("do"), 
        df["do_location_id"] == col("do.location_id"), 
        "left"
    ) \
    .select(col("*"), col("do.location_sk").alias("do_location_sk"))

    # Add a surrogate key column by hashing a combination of key columns
    df = df.withColumn(
        "trip_sk",
        sha2(
            concat_ws(
                "||",
                "vendor_id",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime"
            ),
            256
        )
    )

    df = df.select(
        "trip_sk", 
        "vendor_id",
        "trip_date",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_duration",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_sk",
        "do_location_sk",
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
        "cbd_congestion_fee"
    )

    return df

    
def save_data(df, file_path: str):
    """
    Save a Spark DataFrame to a specified file path in Delta format.

    :param df: The Spark DataFrame to be saved.
    :param file_path: The path where the data should be saved.
    """
    try:
        df.write.format("delta").mode("overwrite").save(file_path) 
        print(f"Data saved to path: {file_path}")
    except Exception as e:
        print(f"Error saving data to path: {file_path}. Error: {e}")
        return
    
def main():
    """
    Main function to execute the data loading, transformation, and saving process.
    """
    try:
        # Load data from the silver layer
        df_silver = load_data(SILVER_PATH)
        # Transform the data
        df_gold = transform_data(df_silver)
    except Exception as e:
        print(f"An error occurred during the data processing: {e}")
        return # Stop execution if any error occurs during loading or transformation
    else:
        # Save the transformed data to the gold layer
        save_data(df_gold, FACT_TRIP_PATH)
        print("Data processing and saving completed successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()