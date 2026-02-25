import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from kafka import KafkaConsumer


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "bronze"],
)
def kafka_micro_batch_to_bronze():
    @task
    def consume_and_batch():
        logging.debug("Starting......")
        consumer = KafkaConsumer(
            "website_events",
            bootstrap_servers=["redpanda-0:9092"],  # Internal Docker Address
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,  # Stop if no new messages for 5 seconds
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        events = []
        for message in consumer:
            events.append(message.value)
            logging.debug(message.value)

        if events:
            s3_hook = S3Hook(aws_conn_id="minio_conn")
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            s3_hook.load_string(
                string_data=json.dumps(events),
                key=f"streaming_batches/batch_{batch_id}.json",
                bucket_name="bronze",
                replace=True,
            )
            print(f"Successfully batched {len(events)} events.")

    consume_and_batch()


kafka_micro_batch_to_bronze()
