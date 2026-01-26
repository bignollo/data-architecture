import os
import requests
from minio import Minio
from minio.error import S3Error

# 1. Configuration
# These match the credentials in your docker-compose.yml
MINIO_URL = "localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"
BUCKET_NAME = "bronze"

# We'll pull January 2024 data as our starting point
#DATASET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
LOCAL_FILES = ["yellow_tripdata_2025-01.parquet", "yellow_tripdata_2025-02.parquet", "yellow_tripdata_2025-03.parquet"]
LOCAL_PATH = "../../data/"

def seed_minio():
    # Initialize MinIO client
    client = Minio(
        MINIO_URL,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False # Use False for local development
    )

    # A. Create the Bronze Bucket if it doesn't exist
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"✅ Created bucket: {BUCKET_NAME}")
        else:
            print(f"ℹ Bucket '{BUCKET_NAME}' already exists.")
    except S3Error as e:
        print(f"❌ Error creating bucket: {e}")
        return

    # B. Download the Dataset
#    if not os.path.exists(LOCAL_FILE):
#        print(f"📥 Downloading NYC Taxi Data (~50MB)...")
#        response = requests.get(DATASET_URL)
#        with open(LOCAL_FILE, 'wb') as f:
#            f.write(response.content)
#        print("✅ Download complete.")
#    else:
#        print("ℹ File already exists locally.")

    # C. Upload to MinIO (The "Bronze" Layer)
    try:
        for LOCAL_FILE in LOCAL_FILES:
            print(f"📤 Uploading {LOCAL_FILE} to {BUCKET_NAME}...")
            client.fput_object(
                BUCKET_NAME,
                f"nyc_taxi/{LOCAL_FILE}",
                "../../data"+LOCAL_FILE
            )
            print(f"🚀 Success! Data is now in your local Data Lake.")
    except S3Error as e:
        print(f"❌ Error uploading file: {e}")

if __name__ == "__main__":
    seed_minio()
