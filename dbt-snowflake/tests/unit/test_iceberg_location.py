from copy import deepcopy

import pytest

from dbt.adapters.snowflake.catalogs import (
    IcebergManagedCatalogIntegration,
    DEFAULT_ICEBERG_CATALOG_INTEGRATION,
)


@pytest.fixture
def fake_integration() -> IcebergManagedCatalogIntegration:
    return IcebergManagedCatalogIntegration(DEFAULT_ICEBERG_CATALOG_INTEGRATION)


model_base = {
    "database": "my_database",
    "schema": "my_schema",
    "alias": "my_table",
    "config": {
        "catalog": "snowflake",
        "external_volume": "s3_iceberg_snow",
    },
}


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
def test_iceberg_base_location_managed(fake_integration, config, expected):
    """Test when base_location_root and base_location_subpath are provided"""
    model = deepcopy(model_base)
    model["config"].update(config)
    relation = fake_integration.build_relation(model)
    assert relation.base_location == expected
