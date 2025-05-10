from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pytest

from dbt.adapters.snowflake.catalogs import (
    IcebergRESTCatalogIntegration,
    IcebergRESTCatalogRelation,
)
from dbt.adapters.snowflake import constants


@dataclass
class FakeCatalogIntegrationConfig:
    name: str = "my_iceberg_rest_catalog_integration"
    catalog_name: str = "my_iceberg_rest_catalog"
    # the above are required, but static (for these tests) parameters
    catalog_type: str = "iceberg_rest"
    table_format: str = constants.ICEBERG_TABLE_FORMAT
    external_volume: Optional[str] = None
    adapter_properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FakeRelationConfig:
    database: str = "my_database"
    schema: str = "my_schema"
    identifier: str = "my_table"
    config: Dict[str, Any] = field(default_factory=dict)


MINIMUM_CATALOG_INTEGRATION_CONFIG = FakeCatalogIntegrationConfig()
MINIMUM_RELATION_CONFIG = FakeRelationConfig()
MAXIMUM_CATALOG_INTEGRATION_CONFIG = FakeCatalogIntegrationConfig(
    external_volume="my_external_volume",
    adapter_properties={
        "catalog_namespace": "my_catalog_namespace",
        "replace_invalid_characters": True,
        "auto_refresh": True,
    },
)
MAXIMUM_RELATION_CONFIG = FakeRelationConfig(
    config={
        "catalog_table": "my_special_catalog_table",
        "catalog_namespace": "my_special_catalog_namespace",
        "external_volume": "my_special_external_volume",
        "replace_invalid_characters": False,
        "auto_refresh": False,
    }
)


DEFAULTS = IcebergRESTCatalogRelation(
    catalog_table=MINIMUM_RELATION_CONFIG.identifier,
    catalog_name=FakeCatalogIntegrationConfig.catalog_name,
    catalog_namespace=None,
    external_volume=None,
    replace_invalid_characters=None,
    auto_refresh=None,
    table_format=constants.ICEBERG_TABLE_FORMAT,
)
CATALOG_DEFAULTS = IcebergRESTCatalogRelation(
    catalog_table=MINIMUM_RELATION_CONFIG.identifier,
    catalog_name=FakeCatalogIntegrationConfig.catalog_name,
    catalog_namespace=MAXIMUM_CATALOG_INTEGRATION_CONFIG.adapter_properties["catalog_namespace"],
    external_volume=MAXIMUM_CATALOG_INTEGRATION_CONFIG.external_volume,
    replace_invalid_characters=MAXIMUM_CATALOG_INTEGRATION_CONFIG.adapter_properties[
        "replace_invalid_characters"
    ],
    auto_refresh=MAXIMUM_CATALOG_INTEGRATION_CONFIG.adapter_properties["auto_refresh"],
    table_format=constants.ICEBERG_TABLE_FORMAT,
)
RELATION_OVERRIDES = IcebergRESTCatalogRelation(
    catalog_table=MAXIMUM_RELATION_CONFIG.config["catalog_table"],
    catalog_name=FakeCatalogIntegrationConfig.catalog_name,
    catalog_namespace=MAXIMUM_RELATION_CONFIG.config["catalog_namespace"],
    external_volume=MAXIMUM_RELATION_CONFIG.config["external_volume"],
    replace_invalid_characters=MAXIMUM_RELATION_CONFIG.config["replace_invalid_characters"],
    auto_refresh=MAXIMUM_RELATION_CONFIG.config["auto_refresh"],
    table_format=constants.ICEBERG_TABLE_FORMAT,
)


@pytest.mark.parametrize(
    "catalog_integration_config,relation_config,expected_relation",
    [
        (MINIMUM_CATALOG_INTEGRATION_CONFIG, MINIMUM_RELATION_CONFIG, DEFAULTS),
        (MAXIMUM_CATALOG_INTEGRATION_CONFIG, MINIMUM_RELATION_CONFIG, CATALOG_DEFAULTS),
        (MINIMUM_CATALOG_INTEGRATION_CONFIG, MAXIMUM_RELATION_CONFIG, RELATION_OVERRIDES),
        (MAXIMUM_CATALOG_INTEGRATION_CONFIG, MAXIMUM_RELATION_CONFIG, RELATION_OVERRIDES),
    ],
)
def test_correct_relations_are_produced(
    catalog_integration_config, relation_config, expected_relation
):
    catalog_integration = IcebergRESTCatalogIntegration(catalog_integration_config)
    assert catalog_integration.build_relation(relation_config) == expected_relation
