from copy import deepcopy
from types import SimpleNamespace

import pytest

from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.catalogs import BuiltInCatalogIntegration


@pytest.fixture
def fake_integration() -> BuiltInCatalogIntegration:
    return BuiltInCatalogIntegration(constants.DEFAULT_BUILT_IN_CATALOG)


model_base = SimpleNamespace(
    database="my_database",
    schema="my_schema",
    identifier="my_table",
    config={
        "catalog": "snowflake",
        "external_volume": "s3_iceberg_snow",
    },
)


@pytest.mark.parametrize(
    "config,expected",
    [
        (
            {},
            "_dbt/my_schema/my_table",
        ),
        (
            {"base_location_root": None, "base_location_subpath": None},
            "_dbt/my_schema/my_table",
        ),
        (
            {"base_location_root": "root_path", "base_location_subpath": "subpath"},
            "root_path/my_schema/my_table/subpath",
        ),
        (
            {"base_location_subpath": "subpath"},
            "_dbt/my_schema/my_table/subpath",
        ),
        (
            {"base_location_root": "root_path"},
            "root_path/my_schema/my_table",
        ),
    ],
)
def test_iceberg_base_location_built_in(fake_integration, config, expected):
    """Test when base_location_root and base_location_subpath are provided"""
    model = deepcopy(model_base)
    model.config.update(config)
    relation = fake_integration.build_relation(model)
    assert relation.base_location == expected


@pytest.mark.parametrize(
    "config,expected",
    [
        (None, None),
        (False, "FALSE"),
        (True, "TRUE"),
        ("False", "FALSE"),
        ("True", "TRUE"),
    ],
)
def test_change_tracking_model_config(fake_integration, config, expected):
    model = deepcopy(model_base)
    model.config.update({"change_tracking": config})
    relation = fake_integration.build_relation(model)
    assert relation.change_tracking == expected


@pytest.mark.parametrize(
    "user_input",
    [
        "0",
        "",
        "None",
    ],
)
def test_change_tracking_invalid_model_config(fake_integration, user_input):
    model = deepcopy(model_base)
    model.config.update({"change_tracking": user_input})
    with pytest.raises(ValueError) as e:
        fake_integration.build_relation(model)
    assert "Invalid value for change_tracking" in str(e.value)
