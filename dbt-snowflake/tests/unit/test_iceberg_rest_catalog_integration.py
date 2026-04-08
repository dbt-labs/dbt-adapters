"""Tests for IcebergRestCatalog integration with Snowflake macros."""

import pytest
from unittest.mock import Mock
from types import SimpleNamespace

from dbt.adapters.catalogs import (
    CatalogIntegrationClient,
    InvalidCatalogIntegrationConfigError,
)
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergRestCatalogIntegration,
    IcebergRestCatalogRelation,
)
from dbt.adapters.snowflake.impl import SnowflakeAdapter


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

    def test_preserve_identifier_case_true_stored_from_adapter_properties(self):
        """preserve_identifier_case=True from adapter_properties is stored on integration."""
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "test_db",
                "preserve_identifier_case": True,
            },
        )
        integration = IcebergRestCatalogIntegration(config)
        assert integration.preserve_identifier_case is True

    def test_preserve_identifier_case_not_set_defaults_none(self):
        """preserve_identifier_case defaults to None when not in adapter_properties."""
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
        assert integration.preserve_identifier_case is None


def _make_mock_adapter(quoting=None, catalog_integrations=None):
    """Create a mock SnowflakeAdapter with _catalog_client and config.quoting.

    Uses SimpleNamespace to avoid Mock(spec=) overriding bound methods.
    """
    adapter = SimpleNamespace()
    adapter.Relation = SnowflakeRelation

    # Bind real methods from SnowflakeAdapter
    adapter._is_quoted = lambda identifier: SnowflakeAdapter._is_quoted(adapter, identifier)
    adapter._strip_quotes = lambda identifier: SnowflakeAdapter._strip_quotes(adapter, identifier)
    adapter._preserve_identifier_case = (
        lambda database: SnowflakeAdapter._preserve_identifier_case(adapter, database)
    )

    # Set up config.quoting
    if quoting is None:
        quoting = {"database": False, "schema": False, "identifier": False}
    adapter.config = SimpleNamespace(quoting=quoting)

    # Set up _catalog_client
    client = CatalogIntegrationClient([IcebergRestCatalogIntegration])
    if catalog_integrations:
        for integration in catalog_integrations:
            client.add(integration)
    adapter._catalog_client = client

    return adapter


class TestPreserveIdentifierCase:
    """Tests for SnowflakeAdapter._preserve_identifier_case()."""

    def test_returns_false_for_regular_database(self):
        adapter = _make_mock_adapter()

        result = SnowflakeAdapter._preserve_identifier_case(adapter, "regular_db")
        assert result is False

    def test_returns_false_when_preserve_identifier_case_not_set(self):
        """Default (None) means case is NOT preserved (standard Snowflake uppercase)."""
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "my_cld",
            },
        )
        adapter = _make_mock_adapter(catalog_integrations=[config])

        result = SnowflakeAdapter._preserve_identifier_case(adapter, "my_cld")
        assert result is False

    def test_returns_true_when_preserve_identifier_case_true(self):
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "my_cld",
                "preserve_identifier_case": True,
            },
        )
        adapter = _make_mock_adapter(catalog_integrations=[config])

        result = SnowflakeAdapter._preserve_identifier_case(adapter, "my_cld")
        assert result is True

    def test_case_insensitive_database_lookup(self):
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "My_CLD",
                "preserve_identifier_case": True,
            },
        )
        adapter = _make_mock_adapter(catalog_integrations=[config])

        assert SnowflakeAdapter._preserve_identifier_case(adapter, "MY_CLD") is True
        assert SnowflakeAdapter._preserve_identifier_case(adapter, "my_cld") is True


class TestMakeMatchKwargs:
    """Tests for SnowflakeAdapter._make_match_kwargs()."""

    def test_uppercases_when_quoting_false(self):
        """Standard behavior: uppercase identifiers when quoting is False."""
        adapter = _make_mock_adapter()

        result = SnowflakeAdapter._make_match_kwargs(adapter, "my_db", "my_schema", "my_table")
        assert result == {
            "database": "MY_DB",
            "schema": "MY_SCHEMA",
            "identifier": "MY_TABLE",
        }

    def test_preserves_case_when_preserve_identifier_case_enabled(self):
        """CLD with preserve_identifier_case=True: preserve lowercase."""
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "my_cld",
                "preserve_identifier_case": True,
            },
        )
        adapter = _make_mock_adapter(catalog_integrations=[config])

        result = SnowflakeAdapter._make_match_kwargs(adapter, "my_cld", "my_schema", "my_table")
        assert result == {
            "database": "my_cld",
            "schema": "my_schema",
            "identifier": "my_table",
        }

    def test_strips_quotes_from_quoted_identifiers(self):
        """Quoted identifiers should be stripped regardless of CLD settings."""
        adapter = _make_mock_adapter()

        result = SnowflakeAdapter._make_match_kwargs(
            adapter, '"my_db"', '"my_schema"', '"my_table"'
        )
        assert result == {
            "database": "my_db",
            "schema": "my_schema",
            "identifier": "my_table",
        }


class TestParseListRelationsResult:
    """Tests for SnowflakeAdapter._parse_list_relations_result()."""

    def _make_result_row(
        self,
        database,
        schema,
        identifier,
        relation_type="TABLE",
        is_dynamic="N",
        is_iceberg="N",
    ):
        """Create a mock agate.Row for _parse_list_relations_result."""
        return (database, schema, identifier, relation_type, is_dynamic, is_iceberg)

    def test_quote_policy_true_for_regular_database(self):
        adapter = _make_mock_adapter()

        row = self._make_result_row("MY_DB", "MY_SCHEMA", "MY_TABLE")
        relation = SnowflakeAdapter._parse_list_relations_result(adapter, row)

        assert relation.quote_policy.database is True
        assert relation.quote_policy.schema is True
        assert relation.quote_policy.identifier is True

    def test_quote_policy_false_for_cld_with_preserve_identifier_case_enabled(self):
        config = SimpleNamespace(
            name="test_catalog",
            catalog_name="TEST",
            catalog_type="iceberg_rest",
            external_volume="s3_volume",
            adapter_properties={
                "catalog_linked_database": "my_cld",
                "preserve_identifier_case": True,
            },
        )
        adapter = _make_mock_adapter(catalog_integrations=[config])

        row = self._make_result_row("my_cld", "my_schema", "my_table")
        relation = SnowflakeAdapter._parse_list_relations_result(adapter, row)

        assert relation.quote_policy.database is False
        assert relation.quote_policy.schema is False
        assert relation.quote_policy.identifier is False
