import pytest
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from dbt.adapters.catalogs import InvalidCatalogIntegrationConfigError
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.redshift import constants
from dbt.adapters.redshift.catalogs import (
    GlueCatalogIntegration,
    RedshiftInfoSchemaCatalogIntegration,
    RedshiftCatalogRelation,
)
from dbt.adapters.redshift import parse_model


class TestGlueCatalogIntegration(unittest.TestCase):
    """Tests for GlueCatalogIntegration."""

    def _make_config(self, **kwargs):
        """Create a catalog integration config with required fields."""
        defaults = {
            "name": "test_glue_catalog",
            "catalog_type": constants.GLUE_CATALOG_TYPE,
            "catalog_name": "test_catalog",
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "file_format": constants.PARQUET_FILE_FORMAT,
            "external_volume": "s3://my-bucket/iceberg-data",
            "adapter_properties": {
                "external_schema": "my_external_schema",
                "glue_database": "my_glue_db",
            },
        }
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    def test_init_with_valid_config(self):
        """Test initialization with valid configuration."""
        config = self._make_config()
        integration = GlueCatalogIntegration(config)

        assert integration.name == "test_glue_catalog"
        assert integration.catalog_type == constants.GLUE_CATALOG_TYPE
        assert integration.table_format == constants.ICEBERG_TABLE_FORMAT
        assert integration.file_format == constants.PARQUET_FILE_FORMAT
        assert integration.external_volume == "s3://my-bucket/iceberg-data"
        assert integration.external_schema == "my_external_schema"
        assert integration.glue_database == "my_glue_db"
        assert integration.allows_writes is True

    def test_init_without_external_schema_raises_error(self):
        """Test that missing external_schema raises an error."""
        config = self._make_config(adapter_properties={"glue_database": "my_glue_db"})

        with pytest.raises(InvalidCatalogIntegrationConfigError) as exc_info:
            GlueCatalogIntegration(config)

        assert "external_schema" in str(exc_info.value)

    def test_init_without_adapter_properties(self):
        """Test that missing adapter_properties raises an error."""
        config = self._make_config(adapter_properties=None)

        with pytest.raises(InvalidCatalogIntegrationConfigError):
            GlueCatalogIntegration(config)

    def test_build_relation_basic(self):
        """Test building a basic catalog relation."""
        config = self._make_config()
        integration = GlueCatalogIntegration(config)

        model = MagicMock(spec=RelationConfig)
        # Use a real dict with at least one key to avoid empty dict issues
        model.config = {"some_config": "value"}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = integration.build_relation(model)

        assert isinstance(relation, RedshiftCatalogRelation)
        assert relation.catalog_type == constants.GLUE_CATALOG_TYPE
        assert relation.catalog_name == "test_glue_catalog"
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.file_format == constants.PARQUET_FILE_FORMAT
        assert relation.external_schema == "my_external_schema"
        assert relation.glue_database == "my_glue_db"
        assert relation.storage_uri == "s3://my-bucket/iceberg-data/test_schema/test_model/"

    def test_build_relation_with_partition_by(self):
        """Test building a relation with partition configuration."""
        config = self._make_config()
        integration = GlueCatalogIntegration(config)

        model = MagicMock(spec=RelationConfig)
        model.config = {"partition_by": ["date_col", "region"]}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = integration.build_relation(model)

        assert relation.partition_by == ["date_col", "region"]

    def test_build_relation_with_string_partition_by(self):
        """Test building a relation with single partition column as string."""
        config = self._make_config()
        integration = GlueCatalogIntegration(config)

        model = MagicMock(spec=RelationConfig)
        model.config = {"partition_by": "date_col"}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = integration.build_relation(model)

        assert relation.partition_by == ["date_col"]

    def test_build_relation_with_explicit_storage_uri(self):
        """Test that explicit storage_uri in model config takes precedence."""
        config = self._make_config()
        integration = GlueCatalogIntegration(config)

        model = MagicMock(spec=RelationConfig)
        model.config = {"storage_uri": "s3://custom-bucket/custom-path/"}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = integration.build_relation(model)

        assert relation.storage_uri == "s3://custom-bucket/custom-path/"

    def test_build_relation_with_model_external_schema_override(self):
        """Test that model-level external_schema overrides catalog default."""
        config = self._make_config()
        integration = GlueCatalogIntegration(config)

        model = MagicMock(spec=RelationConfig)
        model.config = {"external_schema": "custom_external_schema"}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = integration.build_relation(model)

        assert relation.external_schema == "custom_external_schema"


class TestGlueCatalogStorageUriCalculation(unittest.TestCase):
    """Tests for storage URI calculation in GlueCatalogIntegration."""

    def setUp(self):
        config = SimpleNamespace(
            name="test_glue_catalog",
            catalog_type=constants.GLUE_CATALOG_TYPE,
            catalog_name="test_catalog",
            table_format=constants.ICEBERG_TABLE_FORMAT,
            file_format=constants.PARQUET_FILE_FORMAT,
            external_volume="s3://my-bucket/iceberg-data",
            adapter_properties={
                "external_schema": "my_external_schema",
                "glue_database": "my_glue_db",
            },
        )
        self.integration = GlueCatalogIntegration(config)

    def _make_model(self, config_dict=None, schema="my_schema", name="my_model"):
        """Create a model mock with proper config dict behavior."""
        model = MagicMock(spec=RelationConfig)
        # Use a real dict for config so .get() works correctly
        model.config = config_dict if config_dict is not None else {}
        model.schema = schema
        model.name = name
        return model

    def test_storage_uri_auto_generated(self):
        """Test auto-generated storage URI from external_volume."""
        model = self._make_model(config_dict={"some_key": "value"})

        result = self.integration._calculate_storage_uri(model)

        assert result == "s3://my-bucket/iceberg-data/my_schema/my_model/"

    def test_storage_uri_strips_trailing_slash(self):
        """Test that trailing slash in external_volume is handled."""
        self.integration.external_volume = "s3://my-bucket/iceberg-data/"

        model = self._make_model(config_dict={"some_key": "value"})

        result = self.integration._calculate_storage_uri(model)

        assert result == "s3://my-bucket/iceberg-data/my_schema/my_model/"

    def test_storage_uri_explicit_takes_precedence(self):
        """Test that explicit storage_uri takes precedence."""
        model = self._make_model(config_dict={"storage_uri": "s3://custom/path"})

        result = self.integration._calculate_storage_uri(model)

        assert result == "s3://custom/path"

    def test_storage_uri_no_external_volume(self):
        """Test storage URI when no external_volume is configured."""
        self.integration.external_volume = None

        model = self._make_model(config_dict={"some_key": "value"})

        result = self.integration._calculate_storage_uri(model)

        assert result is None

    def test_storage_uri_default_schema(self):
        """Test storage URI when schema is not set."""
        model = self._make_model(config_dict={"some_key": "value"}, schema=None)

        result = self.integration._calculate_storage_uri(model)

        assert result == "s3://my-bucket/iceberg-data/_default/my_model/"


class TestRedshiftInfoSchemaCatalogIntegration(unittest.TestCase):
    """Tests for RedshiftInfoSchemaCatalogIntegration."""

    def test_init(self):
        """Test initialization of info schema catalog."""
        config = SimpleNamespace(
            name="info_schema",
            catalog_type=constants.INFO_SCHEMA_CATALOG_TYPE,
            catalog_name="info_schema",
            table_format=constants.DEFAULT_TABLE_FORMAT,
            file_format=constants.DEFAULT_FILE_FORMAT,
            external_volume=None,
        )
        integration = RedshiftInfoSchemaCatalogIntegration(config)

        assert integration.catalog_type == constants.INFO_SCHEMA_CATALOG_TYPE
        assert integration.table_format == constants.DEFAULT_TABLE_FORMAT
        assert integration.allows_writes is True

    def test_build_relation(self):
        """Test building a relation for standard tables."""
        config = SimpleNamespace(
            name="info_schema",
            catalog_type=constants.INFO_SCHEMA_CATALOG_TYPE,
            catalog_name="info_schema",
            table_format=constants.DEFAULT_TABLE_FORMAT,
            file_format=constants.DEFAULT_FILE_FORMAT,
            external_volume=None,
        )
        integration = RedshiftInfoSchemaCatalogIntegration(config)

        model = MagicMock(spec=RelationConfig)
        model.config = {}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = integration.build_relation(model)

        assert isinstance(relation, RedshiftCatalogRelation)
        assert relation.catalog_type == constants.INFO_SCHEMA_CATALOG_TYPE
        assert relation.table_format == constants.DEFAULT_TABLE_FORMAT
        assert relation.is_iceberg_format is False


class TestRedshiftCatalogRelation(unittest.TestCase):
    """Tests for RedshiftCatalogRelation."""

    def test_is_iceberg_format_true(self):
        """Test is_iceberg_format property when format is iceberg."""
        relation = RedshiftCatalogRelation(table_format=constants.ICEBERG_TABLE_FORMAT)
        assert relation.is_iceberg_format is True

    def test_is_iceberg_format_false(self):
        """Test is_iceberg_format property when format is not iceberg."""
        relation = RedshiftCatalogRelation(table_format=constants.DEFAULT_TABLE_FORMAT)
        assert relation.is_iceberg_format is False

    def test_defaults(self):
        """Test default values."""
        relation = RedshiftCatalogRelation()

        assert relation.catalog_type == constants.INFO_SCHEMA_CATALOG_TYPE
        assert relation.table_format == constants.DEFAULT_TABLE_FORMAT
        assert relation.file_format == constants.DEFAULT_FILE_FORMAT
        assert relation.external_volume is None
        assert relation.storage_uri is None
        assert relation.glue_database is None
        assert relation.external_schema is None
        assert relation.partition_by is None


class TestParseModel(unittest.TestCase):
    """Tests for parse_model helper functions."""

    def test_catalog_name_from_catalog_name_key(self):
        """Test getting catalog name from catalog_name config key."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"catalog_name": "my_catalog"}

        result = parse_model.catalog_name(model)

        assert result == "my_catalog"

    def test_catalog_name_from_catalog_key(self):
        """Test getting catalog name from legacy catalog config key."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"catalog": "my_catalog"}

        result = parse_model.catalog_name(model)

        assert result == "my_catalog"

    def test_catalog_name_default(self):
        """Test that None is returned when no catalog is specified.

        This allows the adapter to fall back to standard Redshift tables
        rather than routing through catalog integration.
        """
        model = MagicMock(spec=RelationConfig)
        model.config = {}

        result = parse_model.catalog_name(model)

        # When no catalog is specified, return None to indicate
        # standard Redshift table (not catalog-managed)
        assert result is None

    def test_external_schema(self):
        """Test getting external schema from config."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"external_schema": "my_schema"}

        result = parse_model.external_schema(model)

        assert result == "my_schema"

    def test_partition_by_list(self):
        """Test getting partition_by as list."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"partition_by": ["col1", "col2"]}

        result = parse_model.partition_by(model)

        assert result == ["col1", "col2"]

    def test_partition_by_string(self):
        """Test getting partition_by as string (normalized to list)."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"partition_by": "col1"}

        result = parse_model.partition_by(model)

        assert result == ["col1"]

    def test_partition_by_none(self):
        """Test partition_by when not specified."""
        model = MagicMock(spec=RelationConfig)
        model.config = {}

        result = parse_model.partition_by(model)

        assert result is None

    def test_storage_uri(self):
        """Test getting storage_uri from config."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"storage_uri": "s3://bucket/path"}

        result = parse_model.storage_uri(model)

        assert result == "s3://bucket/path"

    def test_file_format(self):
        """Test getting file_format from config."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"file_format": "orc"}

        result = parse_model.file_format(model)

        assert result == "orc"

    def test_no_config(self):
        """Test handling model with no config."""
        model = MagicMock(spec=RelationConfig)
        model.config = None

        assert parse_model.catalog_name(model) is None
        assert parse_model.external_schema(model) is None
        assert parse_model.partition_by(model) is None
        assert parse_model.storage_uri(model) is None


if __name__ == "__main__":
    unittest.main()
