import io
import json

from kafka import KafkaConsumer
from minio import Minio

# 1. Setup MinIO Client
client = Minio(
    "localhost:9000", access_key="admin", secret_key="password123", secure=False
)

# 2. Setup Kafka Consumer
consumer = KafkaConsumer(
    "website_events",
    bootstrap_servers=["localhost:19092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("🎧 Listening for real-time events...")

for message in consumer:
    event = message.value
    event_id = f"{event['user_id']}_{int(event['timestamp'])}"

    # Convert dict to bytes for MinIO
    data = json.dumps(event).encode("utf-8")

    # Save directly to Bronze/stream/
    client.put_object(
        "bronze", f"stream/event_{event_id}.json", io.BytesIO(data), length=len(data)
    )
    print(f"✅ Real-time ingestion: Saved event {event_id}")
