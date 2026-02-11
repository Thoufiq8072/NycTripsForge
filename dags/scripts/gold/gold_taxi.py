from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from scripts.spark.spark_session import create_spark_session

spark = create_spark_session("yellow_taxi_silver_loader")    

BASE_PATH = "/mnt/f/airflow-tutorial/data"
SILVER_PATH = f"{BASE_PATH}/silver/trips_data"
GOLD_PATH = f"{BASE_PATH}/gold/trips_data"

def load_data(file_path: str):
    """
    Load data from a specified file path and type into a Spark DataFrame.

    :param file_path: The path to the data file.
    :return: A Spark DataFrame containing the loaded data.
    """
    df = spark.read.format("delta").load(SILVER_PATH)

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

def main():
    """
    Main function to execute the data loading, transformation, and saving process.
    """
    # Load data from the silver layer
    df_silver = load_data(SILVER_PATH)

    # Perform any necessary transformations (if needed)
    #df_gold = df_silver  # Placeholder for transformations

    # Save the transformed data to the gold layer
    # save_data(df_gold, GOLD_PATH)
    df = df_silver.limit(1).toPandas()
    print(df.T)

if __name__ == "__main__":
    main()