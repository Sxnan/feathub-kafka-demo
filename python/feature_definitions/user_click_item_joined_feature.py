from feathub.feathub_client import FeathubClient
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.table.table_descriptor import TableDescriptor


def build_feature(
    feathub_client: FeathubClient,
    user_click_events_table: FeatureTable,
    item_events_table: FeatureTable,
) -> TableDescriptor:
    """
    :param feathub_client: The feathub client.
    :param user_click_events_table: user click events source table.
    :param item_events_table: item events source table.
    :return: The FeatureView that join the user click events and item events.
    """

    item_fields_to_join = [
        "state",
        "primary_category",
        "secondary_category",
        "province_id",
        "city_id",
    ]

    user_click_item_joined_feature = DerivedFeatureView(
        name="user_click_item_joined_feature",
        source=user_click_events_table,
        features=[
            *[
                f"{item_events_table.name}.{item_field}"
                for item_field in item_fields_to_join
            ],
        ],
        keep_source_fields=True,
    )

    return feathub_client.build_features(
        [item_events_table, user_click_item_joined_feature]
    )[1]
