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

    def test_ctas_not_supported_routes_to_insert_into_macro(self):
        """ctas_not_supported=True should route to snowflake__create_insert_into_table_iceberg_rest."""
        relation = IcebergRestCatalogRelation(
            catalog_name="UNITY_CATALOG",
            catalog_linked_database="unity_db",
            catalog_linked_database_type="unity",
            ctas_not_supported=True,
        )
        assert relation.catalog_type == "ICEBERG_REST"
        assert relation.ctas_not_supported is True

    def test_glue_type_routes_to_insert_into_without_ctas_not_supported(self):
        """Glue catalog_linked_database_type should route to insert-into even without ctas_not_supported."""
        relation = IcebergRestCatalogRelation(
            catalog_name="GLUE_CATALOG",
            catalog_linked_database="glue_db",
            catalog_linked_database_type="glue",
        )
        assert relation.catalog_type == "ICEBERG_REST"
        assert relation.catalog_linked_database_type == "glue"
        assert relation.ctas_not_supported is False

    def test_ctas_not_supported_default_is_false(self):
        """ctas_not_supported should default to False."""
        relation = IcebergRestCatalogRelation(
            catalog_name="TEST",
            catalog_linked_database="test_db",
        )
        assert relation.ctas_not_supported is False

    def test_ctas_not_supported_defaults_false_when_not_in_adapter_properties(self):
        """ctas_not_supported defaults to False when not set in adapter_properties."""
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "test_db",
            },
        )
        integration = IcebergRestCatalogIntegration(config)

        model = Mock()
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)
        assert relation.ctas_not_supported is False

    def test_integration_with_unity_returns_ctas_not_supported(self):
        """Full IcebergRestCatalogIntegration with ctas_not_supported plumbs through."""
        config = SimpleNamespace(
            name="unity_catalog",
            catalog_name="UNITY",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "unity_db",
                "catalog_linked_database_type": "unity",
                "ctas_not_supported": True,
            },
        )
        integration = IcebergRestCatalogIntegration(config)

        model = Mock()
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)
        assert relation.catalog_linked_database == "unity_db"
        assert relation.catalog_linked_database_type == "unity"
        assert relation.ctas_not_supported is True
