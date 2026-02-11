from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from scripts.spark.spark_session import create_spark_session

spark = create_spark_session("yellow_taxi_silver_location_loader")    

BASE_PATH = "/opt/airflow/data"
BRONZE_PATH = f"{BASE_PATH}/bronze/location_data"
SILVER_PATH = f"{BASE_PATH}/silver/location_data"

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
        col("LocationID").cast(IntegerType()).alias("location_id"),
        col("Borough").cast(StringType()).alias("borough"),
        col("Zone").cast(StringType()).alias("zone"),
        col("service_zone").cast(StringType()).alias("service_zone"),
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
    df = df.dropDuplicates(["location_id"])

    # Filter out records with invalid or inconsistent data
    df = df.filter((col("location_id").isNotNull()) & (col("borough").isNotNull()) & (col("zone").isNotNull()))

    # Further filter out records with null values in critical columns
    df = df.filter('(location_id IS NOT NULL) AND (borough IS NOT NULL) AND (zone IS NOT NULL) AND (service_zone IS NOT NULL)')
  
    # Null Handling: Fill null values in non-critical columns with default values
    df = df.fillna({
        "service_zone": "Unknown",
    })

    df = df.select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
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
        print("Data processing completed successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()