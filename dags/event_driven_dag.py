import json
import sys

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetWatcher, dag, task

import include.kafka_trigger as incl

print(sys.path)


def apply_message_function(*args, **kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val


trigger = MessageQueueTrigger(
    queue="kafka://redpanda-0:9092/website_events",
    apply_function="incl.apply_fuction",
)

kafka_topic_asset = Asset(
    "kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)


@dag(schedule=[kafka_topic_asset])
def event_driven_dag():
    @task
    def process_message(**context):
        # Extract the triggering asset events from the context
        triggering_asset_events = context["triggering_asset_events"]
        for event in triggering_asset_events[kafka_topic_asset]:
            # Get the message from the TriggerEvent payload
            print(f"Processing message: {event}")

    process_message()


event_driven_dag()
