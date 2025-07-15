from types import SimpleNamespace
from typing import Dict, Optional

import pytest

from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake.parse_model import base_location, cluster_by


def make_model(config: Dict[str, Optional[str]]) -> RelationConfig:
    return SimpleNamespace(  # type:ignore
        database="fake_database",
        schema="fake_schema",
        identifier="fake_table",
        config=config,
    )


@pytest.mark.parametrize(
    "config,expected",
    [
        (
            {"fake_attr": "fake_value"},  # we check if not model.config
            "_dbt/fake_schema/fake_table",
        ),
        (
            {"base_location_root": None, "base_location_subpath": None},
            "_dbt/fake_schema/fake_table",
        ),
        (
            {"base_location_root": "root_path", "base_location_subpath": "subpath"},
            "root_path/fake_schema/fake_table/subpath",
        ),
        (
            {"base_location_subpath": "subpath"},
            "_dbt/fake_schema/fake_table/subpath",
        ),
        (
            {"base_location_root": "root_path"},
            "root_path/fake_schema/fake_table",
        ),
    ],
)
def test_base_location(config, expected):
    model = make_model(config)
    assert base_location(model) == expected


@pytest.mark.parametrize(
    "config,expected",
    [
        ({"cluster_by": "fake_value"}, "fake_value"),
        ({"cluster_by": ["fake_value", "fake_value_1"]}, "fake_value, fake_value_1"),
    ],
)
def test_cluster_by(config, expected):
    model = make_model(config)
    assert cluster_by(model) == expected
