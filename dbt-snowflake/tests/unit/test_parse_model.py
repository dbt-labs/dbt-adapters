from types import SimpleNamespace
from typing import Dict, Optional

import pytest

from dbt.adapters.contracts.relation import RelationConfig
from dbt.artifacts.resources.v1.saved_query import ExportConfig, ExportDestinationType
from dbt.adapters.snowflake.parse_model import base_location, cluster_by, catalog_name


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


def test_catalog_name():
    model = make_model({"catalog_name": "my_catalog"})
    assert catalog_name(model).lower() == "my_catalog"


def test_catalog_name_with_non_model_config():
    exported_config = ExportConfig(export_as=ExportDestinationType.TABLE)
    model = make_model(exported_config)
    assert catalog_name(model) == None
