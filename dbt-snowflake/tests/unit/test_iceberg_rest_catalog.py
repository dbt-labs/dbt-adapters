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
            adapter_properties={"rest_endpoint": "https://example.com/api"},
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
            adapter_properties={"rest_endpoint": "https://example.com/api"},
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

    def test_catalog_linked_database_none_when_not_provided(self):
        """Test that catalog_linked_database is None when not provided in adapter_properties."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={"rest_endpoint": "https://example.com/api"},
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.catalog_linked_database is None
        assert hasattr(integration, "catalog_linked_database")

    def test_catalog_linked_database_none_when_no_adapter_properties(self):
        """Test that catalog_linked_database is None when adapter_properties is None."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties=None,
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.catalog_linked_database is None
        assert hasattr(integration, "catalog_linked_database")

    def test_build_relation_includes_catalog_linked_database(self):
        """Test that build_relation includes catalog_linked_database in the relation."""
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

        # Mock model config
        model = Mock()
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)

        assert isinstance(relation, IcebergRestCatalogRelation)
        assert relation.catalog_linked_database == "custom_database"

    def test_build_relation_catalog_linked_database_none(self):
        """Test that build_relation handles catalog_linked_database being None."""
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
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)

        assert isinstance(relation, IcebergRestCatalogRelation)
        assert relation.catalog_linked_database is None

    @patch.dict(os.environ, {"SNOWFLAKE_CATALOG_LINKED_DATABASE": "env_database"})
    def test_catalog_linked_database_from_environment_variable(self):
        """Test that catalog_linked_database can be set from environment variable."""
        # This tests the pattern where environment variables might be used
        # in the catalogs.yml configuration
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "rest_endpoint": "https://example.com/api",
                "catalog_linked_database": os.getenv("SNOWFLAKE_CATALOG_LINKED_DATABASE"),
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.catalog_linked_database == "env_database"

    @patch.dict(os.environ, {}, clear=True)
    def test_catalog_linked_database_env_var_fallback(self):
        """Test that catalog_linked_database falls back gracefully when env var is not set."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "rest_endpoint": "https://example.com/api",
                "catalog_linked_database": os.getenv(
                    "SNOWFLAKE_CATALOG_LINKED_DATABASE", "default_database"
                ),
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        assert integration.catalog_linked_database == "default_database"


class TestIcebergRestCatalogRelation:
    def test_catalog_relation_defaults(self):
        """Test that IcebergRestCatalogRelation has correct defaults."""
        relation = IcebergRestCatalogRelation()

        assert relation.catalog_type == constants.DEFAULT_ICEBERG_REST_CATALOG.catalog_type
        assert relation.catalog_name == constants.DEFAULT_ICEBERG_REST_CATALOG.name
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.catalog_linked_database is None
        assert relation.external_volume is None
        assert relation.rest_endpoint is None
        assert relation.file_format is None
        assert relation.cluster_by is None
        assert relation.automatic_clustering is False
        assert relation.is_transient is False

    def test_catalog_relation_with_catalog_linked_database(self):
        """Test that IcebergRestCatalogRelation properly stores catalog_linked_database."""
        relation = IcebergRestCatalogRelation(
            catalog_linked_database="custom_database",
            external_volume="test_volume",
            rest_endpoint="https://test.com/api",
        )

        assert relation.catalog_linked_database == "custom_database"
        assert relation.external_volume == "test_volume"
        assert relation.rest_endpoint == "https://test.com/api"
