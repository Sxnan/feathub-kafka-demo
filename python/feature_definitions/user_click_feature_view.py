from datetime import timedelta

from feathub.common import types
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.table.table_descriptor import TableDescriptor

from feature_definitions import user_click_item_joined_feature


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

    f_secondary_category_value_counts = Feature(
        name="secondary_category_value_counts",
        dtype=types.MapType(types.String, types.Int64),
        transform=SlidingWindowTransform(
            expr="secondary_category",
            agg_func="VALUE_COUNTS",
            group_by_keys=["user_id"],
            limit=20,
            window_size=timedelta(hours=1),
            step_size=timedelta(minutes=10),
        ),
        keys=["user_id"],
    )
    f_first_clk_ts = Feature(
        name="first_click_timestamp",
        dtype=types.Int32,
        transform=SlidingWindowTransform(
            expr="UNIX_TIMESTAMP(ts)",
            agg_func="FIRST_VALUE",
            group_by_keys=["user_id"],
            limit=20,
            window_size=timedelta(hours=1),
            step_size=timedelta(minutes=10),
        ),
        keys=["user_id"],
    )
    f_last_clk_ts = Feature(
        name="last_click_timestamp",
        dtype=types.Int32,
        transform=SlidingWindowTransform(
            expr="UNIX_TIMESTAMP(ts)",
            agg_func="LAST_VALUE",
            group_by_keys=["user_id"],
            limit=20,
            window_size=timedelta(hours=1),
            step_size=timedelta(minutes=10),
        ),
        keys=["user_id"],
    )
    f_row_num = Feature(
        name="click_cnt",
        dtype=types.Int32,
        transform=SlidingWindowTransform(
            expr="1",
            agg_func="COUNT",
            group_by_keys=["user_id"],
            limit=20,
            window_size=timedelta(hours=1),
            step_size=timedelta(minutes=10),
        ),
        keys=["user_id"],
    )

    feature_view = SlidingFeatureView(
        name="feature_view",
        source=user_click_item_joined_feature.build_feature(
            feathub_client, user_click_events_table, item_events_table
        ),
        features=[
            f_first_clk_ts,
            f_last_clk_ts,
            f_row_num,
            f_secondary_category_value_counts,
        ],
    )

    user_click_feature_view = DerivedFeatureView(
        name="user_click_feature_view",
        source=feature_view,
        features=[
            f_secondary_category_value_counts.name,
            Feature(
                name="avg_click_interval",
                dtype=types.Float64,
                transform="(last_click_timestamp - first_click_timestamp) / click_cnt",
                keys=["user_id"],
            ),
        ],
    )

    return feathub_client.build_features([user_click_feature_view])[0]
