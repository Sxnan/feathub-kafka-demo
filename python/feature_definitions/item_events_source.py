from feathub.common import types
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor

item_events_schema = Schema(
    [
        "item_id",
        "state",
        "primary_category",
        "secondary_category",
        "province_id",
        "city_id",
        "ts",
    ],
    [
        types.String,
        types.String,
        types.String,
        types.String,
        types.String,
        types.String,
        types.String,
    ],
)


def build_item_events_file_system_source(
    path: str,
) -> TableDescriptor:

    return FileSystemSource(
        name="item_events",
        path=path,
        data_format="json",
        schema=item_events_schema,
        timestamp_field="ts",
        timestamp_format="%Y-%m-%d %H:%M:%S",
        keys=["item_id"],
    )


def build_item_events_kafka_source(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str = "feathub",
    startup_mode: str = "earliest-offset",
):
    return KafkaSource(
        name="item_events_kafka",
        bootstrap_server=bootstrap_servers,
        topic=topic,
        key_format=None,
        value_format="json",
        schema=item_events_schema,
        consumer_group=consumer_group,
        keys=["item_id"],
        timestamp_field="ts",
        timestamp_format="%Y-%m-%d %H:%M:%S",
        startup_mode=startup_mode,
    )
