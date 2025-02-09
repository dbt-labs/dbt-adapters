from dbt.adapters.base.catalog import CatalogIntegrationConfig
import pytest
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.catalog import SnowflakeManagedIcebergCatalogIntegration
from dbt_common.exceptions import DbtValidationError


@pytest.fixture
def catalog_integration():
    return SnowflakeManagedIcebergCatalogIntegration(
        CatalogIntegrationConfig(
            catalog_name="my_catalog",
            integration_name="my_integration",
            table_format="iceberg",
            catalog_type="managed",
            external_volume="s3_iceberg_snow",
            namespace="my_namespace",
        )
    )


@pytest.fixture
def catalog_integration_with_properties():
    return SnowflakeManagedIcebergCatalogIntegration(
        CatalogIntegrationConfig(
            catalog_name="my_catalog",
            integration_name="my_integration",
            table_format="iceberg",
            catalog_type="managed",
            external_volume="s3_iceberg_snow",
            namespace="my_namespace",
            adapter_properties={
                "auto_refresh": "TRUE",
                "replace_invalid_characters": "TRUE",
            },
        )
    )


@pytest.fixture
def iceberg_config() -> dict:
    """Fixture providing standard Iceberg configuration."""
    return {
        "schema": "my_schema",
        "identifier": "my_table",
        "external_volume": "s3_iceberg_snow",
        "base_location_root": "root_path",
        "base_location_subpath": "subpath",
    }


def get_actual_base_location(
    config: dict[str, str], catalog_integration: SnowflakeManagedIcebergCatalogIntegration
) -> str:
    """Get the actual base location from the configuration by parsing the DDL predicates."""

    relation = SnowflakeRelation.create(
        schema=config["schema"],
        identifier=config["identifier"],
    )
    actual_ddl_predicates = catalog_integration.render_ddl_predicates(relation, config).strip()
    actual_base_location = actual_ddl_predicates.split("base_location = ")[1]

    return actual_base_location


def test_iceberg_path_and_subpath(iceberg_config: dict[str, str], catalog_integration):
    """Test when base_location_root and base_location_subpath are provided"""
    expected_base_location = (
        f"'{iceberg_config['base_location_root']}/"
        f"{iceberg_config['schema']}/"
        f"{iceberg_config['identifier']}/"
        f"{iceberg_config['base_location_subpath']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config, catalog_integration) == expected_base_location


def test_iceberg_only_subpath(iceberg_config: dict[str, str], catalog_integration):
    """Test when only base_location_subpath is provided"""
    del iceberg_config["base_location_root"]

    expected_base_location = (
        f"'_dbt/"
        f"{iceberg_config['schema']}/"
        f"{iceberg_config['identifier']}/"
        f"{iceberg_config['base_location_subpath']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config, catalog_integration) == expected_base_location


def test_iceberg_only_path(iceberg_config: dict[str, str], catalog_integration):
    """Test when only base_location_root is provided"""
    del iceberg_config["base_location_subpath"]

    expected_base_location = (
        f"'{iceberg_config['base_location_root']}/"
        f"{iceberg_config['schema']}/"
        f"{iceberg_config['identifier']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config, catalog_integration) == expected_base_location


def test_iceberg_no_path(iceberg_config: dict[str, str], catalog_integration):
    """Test when no base_location_root or is base_location_subpath provided"""
    del iceberg_config["base_location_root"]
    del iceberg_config["base_location_subpath"]

    expected_base_location = (
        f"'_dbt/" f"{iceberg_config['schema']}/" f"{iceberg_config['identifier']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config, catalog_integration) == expected_base_location


def test_managed_iceberg_catalog_with_properties(
    iceberg_config, catalog_integration_with_properties
):
    """Test when properties are provided"""
    assert catalog_integration_with_properties.auto_refresh == "TRUE"
    assert catalog_integration_with_properties.replace_invalid_characters == "TRUE"

    relation = SnowflakeRelation.create(
        schema=iceberg_config["schema"],
        identifier=iceberg_config["identifier"],
    )
    actual_ddl_predicates = catalog_integration_with_properties.render_ddl_predicates(
        relation, iceberg_config
    ).strip()
    assert "auto_refresh = TRUE" in actual_ddl_predicates
    assert "replace_invalid_characters = TRUE" in actual_ddl_predicates


def test_managed_iceberg_catalog_with_invalid_properties():
    with pytest.raises(DbtValidationError):
        SnowflakeManagedIcebergCatalogIntegration(
            CatalogIntegrationConfig(
                catalog_name="my_catalog",
                integration_name="my_integration",
                table_format="iceberg",
                catalog_type="managed",
                external_volume="s3_iceberg_snow",
                namespace="my_namespace",
                adapter_properties={"auto_refresh": "INVALID"},
            )
        )
