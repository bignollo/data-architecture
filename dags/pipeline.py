from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# This is the "Logic" - what we actually want to do
def download_and_upload():
    # 1. The URL of the data we want (NYC Taxi January 2024)
    urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-04.parquet",
    ]

    # 2. Download it into the computer's memory for a moment
    for url in urls:
        print("Downloading data...")
        response = requests.get(url)

        # 3. Use the Airflow "Connection" we made to talk to MinIO
        s3_hook = S3Hook(aws_conn_id="minio_conn")

        # 4. Upload the raw data to our 'bronze' bucket
        s3_hook.load_bytes(
            bytes_data=response.content,
            key="nyc_taxi/" + url[48:],
            bucket_name="bronze",
            replace=True,
        )
    print("Upload complete!")


# This is the "Orchestrator" - the instructions for Airflow
with DAG(
    dag_id="seed_bronze_layer",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # This means "only run when I click play"
    catchup=False,
) as dag:
    # Define the one task we want to run
    task_ingest = PythonOperator(
        task_id="ingest_to_minio", python_callable=download_and_upload
    )
