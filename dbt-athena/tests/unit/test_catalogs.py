"""Unit tests for dbt-athena catalogs.yml v2 support."""

from multiprocessing import get_context
from types import SimpleNamespace

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena import constants
from dbt.adapters.athena import Plugin as AthenaPlugin
from dbt.adapters.athena.catalogs import (
    AthenaInfoSchemaCatalogIntegration,
    GlueCatalogIntegration,
    S3TablesCatalogIntegration,
)
from dbt.adapters.athena.impl import AthenaAdapter

from .constants import AWS_REGION, DATA_CATALOG_NAME, DATABASE_NAME, S3_STAGING_DIR
from .utils import config_from_parts_or_dicts, inject_adapter


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
    """A real AthenaAdapter so __init__ (default catalog registration) is exercised."""
    project_cfg = {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "config-version": 2,
    }
    profile_cfg = {
        "outputs": {
            "test": {
                "type": "athena",
                "s3_staging_dir": S3_STAGING_DIR,
                "region_name": AWS_REGION,
                "database": DATA_CATALOG_NAME,
                "schema": DATABASE_NAME,
            }
        },
        "target": "test",
    }
    config = config_from_parts_or_dicts(project_cfg, profile_cfg)
    instance = AthenaAdapter(config, get_context("spawn"))
    inject_adapter(instance, AthenaPlugin)
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
    def test_bridge_v2_catalog_glue(self, adapter):
        catalog = _v2_catalog(
            "my_glue",
            "glue",
            "iceberg",
            config={"athena": {"external_volume": "s3://bucket/path", "file_format": "parquet"}},
        )
        result = adapter.bridge_v2_catalog(catalog)
        assert result.name == "my_glue"
        assert result.catalog_type == "glue"
        assert result.table_format == "iceberg"
        assert result.external_volume == "s3://bucket/path"
        assert result.file_format == "parquet"

    def test_bridge_v2_catalog_default_table_format_maps_to_hive(self, adapter):
        # The v2 spec's non-Iceberg value is 'default'; Athena's equivalent is 'hive'.
        catalog = _v2_catalog("my_default", "glue", "default", config={"athena": {}})
        result = adapter.bridge_v2_catalog(catalog)
        assert result.table_format == "hive"

    def test_bridge_v2_catalog_unsupported_table_format_raises(self, adapter):
        catalog = _v2_catalog("bad", "glue", "hudi", config={"athena": {}})
        with pytest.raises(DbtRuntimeError, match="unsupported table_format 'hudi'"):
            adapter.bridge_v2_catalog(catalog)

    def test_bridge_v2_catalog_catalog_database_flows_to_write_config(self, adapter):
        catalog = _v2_catalog(
            "my_glue",
            "glue",
            "iceberg",
            config={"athena": {"catalog_database": "my_glue_db"}},
        )
        result = adapter.bridge_v2_catalog(catalog)
        assert result.catalog_database == "my_glue_db"


class TestS3TablesCatalogIntegration:
    def _config(self, **overrides):
        base = dict(
            name="my_s3_tables",
            catalog_name="my_s3_tables",
            catalog_type=constants.S3_TABLES_CATALOG_TYPE,
            table_format=constants.ICEBERG_TABLE_FORMAT,
            external_volume=None,
            file_format=constants.PARQUET_FILE_FORMAT,
            adapter_properties={},
        )
        return SimpleNamespace(**{**base, **overrides})

    def test_build_relation_is_iceberg(self):
        integration = S3TablesCatalogIntegration(self._config())
        relation = integration.build_relation(_model())
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.catalog_type == constants.S3_TABLES_CATALOG_TYPE

    def test_external_volume_is_always_none(self):
        # S3 Tables manages storage; external_volume must never be set on the relation.
        integration = S3TablesCatalogIntegration(self._config(external_volume="s3://ignored"))
        relation = integration.build_relation(_model())
        assert relation.external_volume is None

    def test_allows_writes(self):
        assert S3TablesCatalogIntegration.allows_writes is True

    def test_bridge_v2_catalog_s3_tables(self, adapter):
        catalog = _v2_catalog(
            "my_s3_tables",
            constants.S3_TABLES_CATALOG_TYPE,
            "iceberg",
            config={"athena": {"file_format": "parquet"}},
        )
        result = adapter.bridge_v2_catalog(catalog)
        assert result.catalog_type == constants.S3_TABLES_CATALOG_TYPE
        assert result.table_format == constants.ICEBERG_TABLE_FORMAT
        assert result.external_volume is None

    def test_registration_and_relation_lookup(self, adapter):
        adapter.add_catalog_integration(self._config())
        relation = adapter.build_catalog_relation(_model({"catalog_name": "my_s3_tables"}))
        assert relation is not None
        assert relation.catalog_type == constants.S3_TABLES_CATALOG_TYPE
        assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
        assert relation.external_volume is None

    def test_is_s3_tables_database(self, adapter):
        # S3 Tables buckets surface as Glue federated catalogs "s3tablescatalog/<bucket>".
        assert adapter.is_s3_tables_database("s3tablescatalog/my-bucket") is True
        # Prefix detection is case-insensitive.
        assert adapter.is_s3_tables_database("S3TablesCatalog/my-bucket") is True
        assert adapter.is_s3_tables_database("awsdatacatalog") is False
        assert adapter.is_s3_tables_database("glue") is False
        assert adapter.is_s3_tables_database(None) is False
        assert adapter.is_s3_tables_database("") is False


class TestCatalogDatabase:
    def test_catalog_database_flows_to_relation(self):
        config = SimpleNamespace(
            name="my_glue",
            catalog_name="my_glue",
            catalog_type=constants.GLUE_CATALOG_TYPE,
            table_format=constants.ICEBERG_TABLE_FORMAT,
            external_volume=None,
            file_format=constants.PARQUET_FILE_FORMAT,
            catalog_database="analytics_db",
            adapter_properties={},
        )
        integration = GlueCatalogIntegration(config)
        assert integration.catalog_database == "analytics_db"
        relation = integration.build_relation(_model())
        assert relation.catalog_database == "analytics_db"

    def test_catalog_database_defaults_to_none(self):
        integration = GlueCatalogIntegration(constants.DEFAULT_GLUE_CATALOG)
        assert integration.catalog_database is None
        relation = integration.build_relation(_model())
        assert relation.catalog_database is None

    def test_info_schema_catalog_database_flows_to_relation(self):
        config = SimpleNamespace(
            name="my_info_schema",
            catalog_name="my_info_schema",
            catalog_type=constants.INFO_SCHEMA_CATALOG_TYPE,
            table_format=constants.HIVE_TABLE_FORMAT,
            external_volume=None,
            file_format=constants.PARQUET_FILE_FORMAT,
            catalog_database="analytics_db",
            adapter_properties={},
        )
        integration = AthenaInfoSchemaCatalogIntegration(config)
        assert integration.catalog_database == "analytics_db"
        relation = integration.build_relation(_model())
        assert relation.catalog_database == "analytics_db"

    def test_catalog_database_flows_from_registration_to_relation(self, adapter):
        # Register a catalog with catalog_database set; build_catalog_relation must
        # propagate it onto the relation so default__generate_database_name can read it.
        config = SimpleNamespace(
            name="db_override_catalog",
            catalog_name="db_override_catalog",
            catalog_type=constants.GLUE_CATALOG_TYPE,
            table_format=constants.ICEBERG_TABLE_FORMAT,
            external_volume=None,
            file_format=constants.PARQUET_FILE_FORMAT,
            catalog_database="my_override_db",
            adapter_properties={},
        )
        adapter.add_catalog_integration(config)
        relation = adapter.build_catalog_relation(_model({"catalog_name": "db_override_catalog"}))
        assert relation is not None
        assert relation.catalog_database == "my_override_db"

    def test_catalog_database_is_none_when_unset(self, adapter):
        relation = adapter.build_catalog_relation(_model({"catalog_name": "glue"}))
        assert relation is not None
        assert relation.catalog_database is None
