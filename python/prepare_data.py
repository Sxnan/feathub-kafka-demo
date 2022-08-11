import os

from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.kafka_sink import KafkaSink

from feature_definitions.item_events_source import build_item_events_file_system_source
from feature_definitions.user_click_events_source import (
    build_user_click_events_file_system_source,
)

file_source_data_dir = "/tmp/data"
user_click_events_source_table = os.path.abspath(
    os.path.join(file_source_data_dir, "user_click_events.json")
)
item_events_source_table = os.path.abspath(
    os.path.join(file_source_data_dir, "item_events.json")
)


def prepare_data():
    client = FeathubClient(
        config={
            "processor": {
                "processor_type": "flink",
                "flink": {"rest.address": "localhost", "rest.port": "8081"},
            },
            "online_store": {
                "memory": {},
            },
            "registry": {
                "registry_type": "local",
                "local": {
                    "namespace": "default",
                },
            },
            "feature_service": {
                "service_type": "local",
                "local": {},
            },
        }
    )

    user_click_events_table = build_user_click_events_file_system_source(
        user_click_events_source_table
    )
    client.materialize_features(
        user_click_events_table,
        KafkaSink(
            bootstrap_server="kafka:9092",
            topic="user_click_events",
            key_format=None,
            value_format="json",
        ),
        allow_overwrite=True,
    ).wait()

    category_event_table = build_item_events_file_system_source(
        item_events_source_table
    )
    client.materialize_features(
        category_event_table,
        KafkaSink(
            bootstrap_server="kafka:9092",
            topic="item_events",
            key_format=None,
            value_format="json",
        ),
        allow_overwrite=True,
    ).wait()


if __name__ == "__main__":
    prepare_data()
