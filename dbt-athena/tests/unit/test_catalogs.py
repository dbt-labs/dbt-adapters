"""Unit tests for dbt-athena catalogs.yml v2 support."""

from types import SimpleNamespace

import pytest

from dbt.adapters.athena import constants
from dbt.adapters.athena.catalogs import (
    AthenaInfoSchemaCatalogIntegration,
    GlueCatalogIntegration,
)
from dbt.adapters.athena.impl import AthenaAdapter
from dbt.adapters.catalogs import CatalogIntegrationClient


def _v2_catalog(name, catalog_type, table_format_value, config=None):
    return SimpleNamespace(
        name=name,
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
        config=config or {},
    )


def _model(config=None):
    return SimpleNamespace(
        database="my_database",
        schema="my_schema",
        identifier="my_table",
        name="my_table",
        config=config or {},
    )


@pytest.fixture
def adapter():
    """Bare adapter with only the catalog client wired up (no connections)."""
    instance = object.__new__(AthenaAdapter)
    instance._catalog_client = CatalogIntegrationClient(AthenaAdapter.CATALOG_INTEGRATIONS)
    instance.add_catalog_integration(constants.DEFAULT_INFO_SCHEMA_CATALOG)
    instance.add_catalog_integration(constants.DEFAULT_GLUE_CATALOG)
    return instance


class TestCatalogIntegrations:
    def test_glue_build_relation_is_iceberg(self):
        integration = GlueCatalogIntegration(constants.DEFAULT_GLUE_CATALOG)
        relation = integration.build_relation(_model())
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.file_format == constants.PARQUET_FILE_FORMAT
        assert relation.catalog_type == constants.GLUE_CATALOG_TYPE

    def test_glue_allows_writes(self):
        assert GlueCatalogIntegration.allows_writes is True

    def test_info_schema_build_relation_is_hive(self):
        integration = AthenaInfoSchemaCatalogIntegration(constants.DEFAULT_INFO_SCHEMA_CATALOG)
        relation = integration.build_relation(_model())
        assert relation.table_format == constants.HIVE_TABLE_FORMAT
        assert relation.catalog_type == constants.INFO_SCHEMA_CATALOG_TYPE


class TestRegistration:
    def test_default_catalogs_registered(self, adapter):
        assert (
            adapter.get_catalog_integration("glue").table_format == constants.ICEBERG_TABLE_FORMAT
        )
        assert (
            adapter.get_catalog_integration("info_schema").table_format
            == constants.HIVE_TABLE_FORMAT
        )

    def test_build_catalog_relation_by_name(self, adapter):
        relation = adapter.build_catalog_relation(_model({"catalog_name": "glue"}))
        assert relation is not None
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT

    def test_build_catalog_relation_legacy_catalog_key(self, adapter):
        relation = adapter.build_catalog_relation(_model({"catalog": "glue"}))
        assert relation is not None
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT

    def test_build_catalog_relation_no_catalog_returns_none(self, adapter):
        # With no catalog referenced, the base adapter returns None and the macro
        # layer falls back to the default 'hive' table_type.
        assert adapter.build_catalog_relation(_model({})) is None


class TestV2Bridge:
    def setup_method(self):
        self.adapter = object.__new__(AthenaAdapter)

    def test_v2_to_v1_type_glue(self):
        assert self.adapter._v2_to_v1_type("glue") == "glue"

    def test_v2_to_v1_type_unknown_passthrough(self):
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"

    def test_bridge_v2_catalog_glue(self):
        catalog = _v2_catalog(
            "my_glue",
            "glue",
            "iceberg",
            config={"athena": {"external_volume": "s3://bucket/path", "file_format": "parquet"}},
        )
        result = self.adapter.bridge_v2_catalog(catalog)
        assert result.name == "my_glue"
        assert result.catalog_type == "glue"
        assert result.table_format == "iceberg"
        assert result.external_volume == "s3://bucket/path"
        assert result.file_format == "parquet"
