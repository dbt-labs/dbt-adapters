import pytest
from unittest.mock import Mock
from types import SimpleNamespace

from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergRestCatalogIntegration,
    IcebergRestCatalogRelation,
)
from dbt.adapters.snowflake import constants


class TestIcebergRestCatalogIntegration:
    def test_catalog_integration_initialization(self):
        """Test that IcebergRestCatalogIntegration initializes correctly."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={"rest_endpoint": "https://example.com/api"},
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.name == "test_iceberg_rest"
        assert integration.catalog_name == "POLARIS"
        assert integration.catalog_type == constants.ICEBERG_REST_CATALOG_TYPE
        assert integration.external_volume == "s3_volume"
        assert integration.adapter_properties["rest_endpoint"] == "https://example.com/api"
        assert integration.allows_writes is True
        assert integration.table_format == constants.ICEBERG_TABLE_FORMAT

    def test_build_relation(self):
        """Test that build_relation creates the correct relation object."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={"rest_endpoint": "https://example.com/api"},
        )

        integration = IcebergRestCatalogIntegration(config)

        # Mock model config
        model = Mock()
        model.config = {
            "base_location_root": "custom_root",
            "base_location_subpath": "subpath",
            "external_volume": "model_volume",
            "cluster_by": ["col1", "col2"],
            "automatic_clustering": True,
        }
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)

        assert isinstance(relation, IcebergRestCatalogRelation)
        assert relation.catalog_name == "POLARIS"
        assert relation.catalog_type == constants.ICEBERG_REST_CATALOG_TYPE
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.external_volume == "model_volume"  # Should use model's volume
        assert relation.base_location == "custom_root/test_schema/test_table/subpath"

    def test_build_relation_with_defaults(self):
        """Test build_relation with minimal model config."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={},
        )

        integration = IcebergRestCatalogIntegration(config)

        # Mock model with minimal config
        model = Mock()
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)

        assert isinstance(relation, IcebergRestCatalogRelation)
        assert relation.catalog_name == "POLARIS"
        assert relation.external_volume == "s3_volume"  # Should use integration's volume
        assert relation.base_location == "_dbt/test_schema/test_table"  # Default prefix
