"""Tests for IcebergRestCatalog integration with Snowflake macros."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from types import SimpleNamespace

from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergRestCatalogIntegration,
    IcebergRestCatalogRelation,
)
from dbt.adapters.catalogs import InvalidCatalogIntegrationConfigError
from dbt.adapters.exceptions.compilation import InvalidRelationConfigError


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
        assert hasattr(relation, "iceberg_version")

        # Verify values
        assert relation.catalog_name == "POLARIS"
        assert relation.catalog_linked_database == "custom_db"
        assert relation.external_volume == "test_volume"
        assert relation.auto_refresh is True

    def test_iceberg_version_from_model_config(self):
        """Test that iceberg_version is passed through from model config."""
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

        model = Mock()
        model.config = {"iceberg_version": 3}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)
        assert relation.iceberg_version == 3

    def test_iceberg_version_from_catalog_config(self):
        """Test that iceberg_version defaults from catalog adapter_properties."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "custom_database",
                "iceberg_version": 3,
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        model = Mock()
        model.config = {}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)
        assert relation.iceberg_version == 3

    def test_iceberg_version_model_overrides_catalog(self):
        """Test that model-level iceberg_version overrides catalog default."""
        config = SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "custom_database",
                "iceberg_version": 1,
            },
        )

        integration = IcebergRestCatalogIntegration(config)

        model = Mock()
        model.config = {"iceberg_version": 3}
        model.schema = "test_schema"
        model.identifier = "test_table"

        relation = integration.build_relation(model)
        assert relation.iceberg_version == 3

    def test_iceberg_version_defaults_to_none(self):
        """Test that iceberg_version defaults to None when not set."""
        relation = IcebergRestCatalogRelation(
            catalog_name="POLARIS",
            catalog_linked_database="custom_db",
        )
        assert relation.iceberg_version is None


class TestIcebergRestBaseLocation:
    """BASE_LOCATION is opt-in for CLDs: required for vended credentials, no default."""

    @staticmethod
    def _integration(**adapter_properties):
        return IcebergRestCatalogIntegration(
            SimpleNamespace(
                name="test_iceberg_rest",
                catalog_name="POLARIS",
                catalog_type="iceberg_rest",
                external_volume=None,
                adapter_properties={"catalog_linked_database": "cld", **adapter_properties},
            )
        )

    @staticmethod
    def _model(config=None):
        model = Mock()
        model.config = config or {}
        model.schema = "my_schema"
        model.identifier = "my_table"
        return model

    def test_no_base_location_by_default(self):
        relation = self._integration().build_relation(self._model())
        assert relation.base_location is None

    def test_catalog_root(self):
        relation = self._integration(base_location_root="s3://bucket/prefix").build_relation(
            self._model()
        )
        assert relation.base_location == "s3://bucket/prefix/my_schema/my_table"

    def test_model_root_overrides_catalog_root(self):
        relation = self._integration(base_location_root="s3://bucket/a").build_relation(
            self._model({"base_location_root": "s3://bucket/b"})
        )
        assert relation.base_location == "s3://bucket/b/my_schema/my_table"

    def test_model_root_and_subpath(self):
        relation = self._integration().build_relation(
            self._model({"base_location_root": "s3://bucket/b", "base_location_subpath": "v1"})
        )
        assert relation.base_location == "s3://bucket/b/my_schema/my_table/v1"

    def test_trailing_slashes_and_whitespace_stripped(self):
        relation = self._integration(base_location_root=" s3://bucket/a// ").build_relation(
            self._model({"base_location_subpath": "/v1/"})
        )
        assert relation.base_location == "s3://bucket/a/my_schema/my_table/v1"

    def test_subpath_without_root_errors(self):
        with pytest.raises(InvalidRelationConfigError):
            self._integration().build_relation(self._model({"base_location_subpath": "v1"}))

    @pytest.mark.parametrize("root", ["", "  ", "/", " // "])
    def test_empty_model_root_errors(self, root):
        with pytest.raises(InvalidRelationConfigError):
            self._integration().build_relation(self._model({"base_location_root": root}))

    @pytest.mark.parametrize("root", ["", "  ", "/"])
    def test_empty_catalog_root_errors(self, root):
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            self._integration(base_location_root=root)
