from airflow.datasets import Dataset

# Bronze datasets
bronze_trips_dataset = Dataset("/warehouse/bronze/trips_data")
bronze_location_dataset = Dataset("/warehouse/bronze/location_data")

# Silver datasets
silver_trips_dataset = Dataset("/warehouse/silver/trips_data")
silver_location_dataset = Dataset("/warehouse/silver/location_data")