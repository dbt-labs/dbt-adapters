import pytest
import os
from unittest.mock import Mock, patch
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
            adapter_properties={
                "rest_endpoint": "https://example.com/api",
                "catalog_linked_database": "custom_db",
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.name == "test_iceberg_rest"
        assert integration.catalog_name == "POLARIS"
        assert integration.catalog_type == constants.ICEBERG_REST_CATALOG_TYPE
        assert integration.external_volume == "s3_volume"
        assert integration.rest_endpoint == "https://example.com/api"
        assert integration.allows_writes is True
        assert integration.table_format == constants.ICEBERG_TABLE_FORMAT

    def test_build_relation(self):
        """Test that build_relation creates the correct relation object."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "rest_endpoint": "https://example.com/api",
                "catalog_linked_database": "custom_db",
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        # Mock model config
        model = Mock()
        model.config = {
            "rest_endpoint": "https://example.com/api",
            "external_volume": "model_volume",
        }
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)

        assert isinstance(relation, IcebergRestCatalogRelation)
        assert relation.catalog_name == "POLARIS"
        assert relation.catalog_type == constants.ICEBERG_REST_CATALOG_TYPE
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.external_volume == "model_volume"  # Should use model's volume
        assert relation.rest_endpoint == "https://example.com/api"

    def test_build_relation_with_defaults(self):
        """Test build_relation with minimal model config."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "rest_endpoint": "https://example.com/api",
                "catalog_linked_database": "custom_db",
            },
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

    def test_catalog_linked_database_initialization(self):
        """Test that catalog_linked_database is properly initialized from adapter_properties."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "rest_endpoint": "https://example.com/api",
                "catalog_linked_database": "custom_database",
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.catalog_linked_database == "custom_database"
        assert hasattr(integration, "catalog_linked_database")
