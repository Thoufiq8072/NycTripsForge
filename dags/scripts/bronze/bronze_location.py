from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from scripts.spark.spark_session import create_spark_session

spark = create_spark_session("yellow_taxi_bronze_location_loader")    

BASE_PATH = "/opt/airflow/data"
RAW_PATH = f"{BASE_PATH}/raw/location/taxi_zone_lookup.csv"
BRONZE_PATH = f"{BASE_PATH}/bronze/location_data"

def load_data(file_path: str, file_type: str):
    """
    Load data from a specified file path and type into a Spark DataFrame.

    :param file_path: The path to the data file.
    :param file_type: The type of the data file (e.g., 'csv', 'json', 'parquet').
    :return: A Spark DataFrame containing the loaded data.
    """
    if file_type == 'csv':
        df = spark.read.csv(file_path, header=True, inferSchema=True, sep=',')
    elif file_type == 'json':
        df = spark.read.json(file_path)
    elif file_type == 'parquet':
        df = spark.read.parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    return df

def main():
    try:
        df = load_data(RAW_PATH, "csv")
        df = df.withColumn("_ingested_date", current_date())\
                .withColumn("_data_source", lit(RAW_PATH)) \
                .withColumn("_load_timestamp", current_timestamp())
    except Exception as e:
        print(f"Error loading data: {e}")
        return  # Stop execution if data loading fails
    else:
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(BRONZE_PATH)
        print("Data loading completed successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
    