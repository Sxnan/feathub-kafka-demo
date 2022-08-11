from feathub.common import types
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.table.schema import Schema

user_click_events_schema = Schema(
    [
        "user_id",
        "location",
        "item_id",
        "device",
        "ts",
    ],
    [
        types.String,
        types.String,
        types.String,
        types.String,
        types.String,
    ],
)


def build_user_click_events_file_system_source(
    path: str,
) -> FeatureTable:

    return FileSystemSource(
        name="user_click_events",
        path=path,
        data_format="json",
        schema=user_click_events_schema,
        timestamp_field="ts",
        timestamp_format="%Y-%m-%d %H:%M:%S",
        keys=["user_id"],
    )


def build_user_click_events_kafka_source(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str = "feathub",
    startup_mode: str = "earliest-offset",
) -> FeatureTable:
    return KafkaSource(
        name="user_click_event_kafka",
        bootstrap_server=bootstrap_servers,
        topic=topic,
        key_format=None,
        value_format="json",
        schema=user_click_events_schema,
        consumer_group=consumer_group,
        keys=["user_id"],
        timestamp_field="ts",
        timestamp_format="%Y-%m-%d %H:%M:%S",
        startup_mode=startup_mode,
    )
