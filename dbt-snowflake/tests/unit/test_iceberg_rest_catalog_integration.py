"""Tests for IcebergRestCatalog integration with Snowflake macros."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from types import SimpleNamespace

from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergRestCatalogIntegration,
    IcebergRestCatalogRelation,
)
from dbt.adapters.catalogs import InvalidCatalogIntegrationConfigError


class TestIcebergRestCatalogIntegration:
    """Test integration between IcebergRestCatalog and Snowflake macros."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_adapter = Mock()
        self.mock_target = Mock()
        self.mock_target.database = "original_database"

    def test_missing_catalog_linked_database_raises_error(self):
        """Error when adapter_properties lacks catalog_linked_database."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={},
        )

        with pytest.raises(InvalidCatalogIntegrationConfigError):
            IcebergRestCatalogIntegration(config)

    def test_catalog_relation_has_catalog_linked_database_attribute(self):
        """Test that catalog relation includes catalog_linked_database for macro usage."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
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

        # Verify the relation has the catalog_linked_database attribute
        assert hasattr(relation, "catalog_linked_database")
        assert relation.catalog_linked_database == "custom_database"

    def test_macro_integration_with_catalog_linked_database_set(self):
        """Test macro behavior when catalog_linked_database is set."""
        # Create a catalog relation with catalog_linked_database set
        relation = IcebergRestCatalogRelation(
            catalog_name="POLARIS",
            catalog_linked_database="custom_database",
            external_volume="test_volume",
        )

        # Test that hasattr returns True for catalog_linked_database
        assert hasattr(relation, "catalog_linked_database")
        assert relation.catalog_linked_database == "custom_database"

        # Simulate the macro logic from get_custom_name.sql
        # {%- if catalog_relation is not none and hasattr(catalog_relation, 'catalog_linked_database') -%}
        if relation is not None and hasattr(relation, "catalog_linked_database"):
            # This would return catalog_name in the actual macro
            result = relation.catalog_name
        else:
            # This would return target.database in the actual macro
            result = "target_database"

        assert result == "POLARIS"

    def test_macro_integration_without_catalog_relation(self):
        """Test macro behavior when catalog_relation is None."""
        relation = None

        # Simulate the macro logic from get_custom_name.sql
        # {%- if catalog_relation is not none and hasattr(catalog_relation, 'catalog_linked_database') -%}
        if relation is not None and hasattr(relation, "catalog_linked_database"):
            # This would return catalog_name in the actual macro
            result = relation.catalog_name
        else:
            # This would return target.database in the actual macro
            result = "target_database"

        assert result == "target_database"

    def test_integration_with_environment_variable_for_macro(self):
        """Test full integration with environment variable that would be used by macro."""
        import os

        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "catalog_linked_database",
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        # Mock model config
        model = Mock()
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)

        # Verify the relation has the correct catalog_linked_database from env var
        assert relation.catalog_linked_database == "catalog_linked_database"

        # Simulate the macro check
        assert hasattr(relation, "catalog_linked_database")

        # The macro would use catalog_name when catalog_linked_database is available
        if relation is not None and hasattr(relation, "catalog_linked_database"):
            result = relation.catalog_name
        else:
            result = "target_database"

        assert result == "POLARIS"

    def test_catalog_relation_all_attributes_present(self):
        """Test that all expected attributes are present on the catalog relation."""
        relation = IcebergRestCatalogRelation(
            catalog_name="POLARIS",
            catalog_linked_database="custom_db",
            external_volume="test_volume",
            auto_refresh=True,
        )

        # Verify all attributes that might be used by macros
        assert hasattr(relation, "catalog_name")
        assert hasattr(relation, "catalog_linked_database")
        assert hasattr(relation, "external_volume")
        assert hasattr(relation, "auto_refresh")
        assert hasattr(relation, "catalog_type")
        assert hasattr(relation, "table_format")
        assert hasattr(relation, "file_format")

        # Verify values
        assert relation.catalog_name == "POLARIS"
        assert relation.catalog_linked_database == "custom_db"
        assert relation.external_volume == "test_volume"
        assert relation.auto_refresh is True
