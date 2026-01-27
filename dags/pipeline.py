from datetime import datetime

import duckdb
import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transformation", "silver"],
)
def taxi_data_silver_layer():
    @task
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

    @task
    def clean_taxi_data():
        # 1. Start DuckDB (in-memory)
        conn = duckdb.connect(":memory:")

        # 2. Configure it to talk to your MinIO
        # Note: We use the internal Docker hostname 'minio'
        conn.execute("""
            INSTALL s3;
            LOAD s3;
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='admin';
            SET s3_secret_access_key='password123';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # 3. The Transformation Logic (The "Architect's Filter")
        # We are removing trips with 0 passengers and 0 distance
        # and ensuring the dates are valid.
        transformation_sql = """
            COPY (
                SELECT
                    VendorID as vendor_id,
                    tpep_pickup_datetime as pickup_time,
                    tpep_dropoff_datetime as dropoff_time,
                    passenger_count,
                    trip_distance,
                    fare_amount
                FROM read_parquet('s3://bronze/nyc_taxi/*.parquet')
                WHERE passenger_count > 0
                  AND trip_distance > 0
                  AND fare_amount > 0
            ) TO 's3://silver/cleaned_taxi_data.parquet' (FORMAT 'PARQUET');
        """

        print("Starting transformation...")
        conn.execute(transformation_sql)
        print("Silver layer file created successfully!")
        # Query the new Silver file to verify the count
        result = conn.execute(
            "SELECT count(*) FROM 's3://silver/cleaned_taxi_data.parquet'"
        ).fetchone()

        # This will show up in the Airflow "Task Logs"
        print(f"--- TRANSFORMATION SUMMARY ---")
        print(f"Total rows saved to Silver: {result[0]}")
        print(f"-------------------------------")

    #    download_and_upload()
    #    clean_taxi_data()
    @task
    def create_business_metrics():
        conn = duckdb.connect(":memory:")

        # Configure MinIO connection
        conn.execute("""
            INSTALL s3;
            LOAD s3;
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='admin';
            SET s3_secret_access_key='password123';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # GOLD LOGIC: Aggregate metrics by Vendor
        # We calculate total revenue and average fare
        gold_sql = """
            COPY (
                SELECT
                    vendor_id,
                    count(*) as total_trips,
                    round(sum(fare_amount), 2) as total_revenue,
                    round(avg(fare_amount), 2) as avg_fare_per_trip,
                    round(avg(trip_distance), 2) as avg_distance
                FROM read_parquet('s3://silver/cleaned_taxi_data.parquet')
                GROUP BY vendor_id
            ) TO 's3://gold/vendor_performance.parquet' (FORMAT 'PARQUET');
        """

        print("Creating Gold Layer metrics...")
        conn.execute(gold_sql)

        # Log a preview for the architect
        stats = conn.execute(
            "SELECT * FROM 's3://gold/vendor_performance.parquet'"
        ).df()
        print(stats)

    download_and_upload() >> clean_taxi_data() >> create_business_metrics()


taxi_data_silver_layer()
