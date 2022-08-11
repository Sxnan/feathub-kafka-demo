from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.kafka_sink import KafkaSink

from feature_definitions import (
    user_click_feature_view,
)
from feature_definitions.item_events_source import build_item_events_kafka_source
from feature_definitions.user_click_events_source import (
    build_user_click_events_kafka_source,
)

if __name__ == "__main__":
    client = FeathubClient(
        config={
            "processor": {
                "processor_type": "flink",
                "flink": {
                    "deployment_mode": "cli",
                    "flink.table.exec.resource.default-parallelism": "1",
                },
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

    bootstrap_servers = "kafka:9092"
    user_click_events_kafka = build_user_click_events_kafka_source(
        bootstrap_servers, "user_click_events"
    )
    item_events_kafka = build_item_events_kafka_source(bootstrap_servers, "item_events")

    features = client.get_features(
        user_click_feature_view.build_feature(
            client, user_click_events_kafka, item_events_kafka
        )
    )
    features.execute_insert(
        KafkaSink(
            bootstrap_server=bootstrap_servers,
            topic="user_click_features",
            key_format=None,
            value_format="json",
        ),
        allow_overwrite=True,
    )
