import json
import random
import time

from kafka import KafkaProducer

# Connect to Redpanda
producer = KafkaProducer(
    bootstrap_servers=["localhost:19092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

actions = ["view_page", "add_to_cart", "purchase", "click_ad"]

print("🚀 Starting stream... Watch http://localhost:8082")

try:
    while True:
        data = {
            "user_id": random.randint(1000, 9999),
            "action": random.choice(actions),
            "timestamp": time.time(),
        }
        producer.send("website_events", value=data)
        print(f"Sent: {data}")
        time.sleep(1)  # Send one event per second
except KeyboardInterrupt:
    print("Stopped.")
