from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

from dbt.adapters.contracts.relation import MaterializationConfig, RelationConfig
from dbt.adapters.snowflake.catalogs import (
    IcebergManagedCatalogIntegration,
    DEFAULT_ICEBERG_CATALOG_INTEGRATION,
)


@pytest.fixture
def fake_integration() -> IcebergManagedCatalogIntegration:
    return IcebergManagedCatalogIntegration(DEFAULT_ICEBERG_CATALOG_INTEGRATION)


@dataclass
class FakeMaterializationConfig:
    extra: Dict[str, Any]


@dataclass
class FakeRelationConfig:
    resource_type: str = ""
    name: str = ""
    description: str = ""
    database: str = "my_database"
    schema: str = "my_schema"
    identifier: str = "my_table"
    compiled_code: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    quoting_dict: Dict[str, bool] = field(default_factory=dict)
    config: Optional[MaterializationConfig] = field(default_factory=FakeMaterializationConfig)
    catalog: Optional[str] = "snowflake"

    external_volume: Optional[str] = "s3_iceberg_snow"
    base_location_root: Optional[str] = None
    base_location_subpath: Optional[str] = None


@pytest.mark.parametrize(
    "config,expected",
    [
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
        (
            {},
            "_dbt/my_schema/my_table",
        ),
    ],
)
def test_iceberg_base_location_managed(fake_integration, config, expected):
    """Test when base_location_root and base_location_subpath are provided"""
    materialization_config = FakeMaterializationConfig(extra=config)
    relation_config = FakeRelationConfig(config=materialization_config)
    relation = fake_integration.build_relation(relation_config)
    assert relation.base_location == expected
