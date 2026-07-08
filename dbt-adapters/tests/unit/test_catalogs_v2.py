"""Unit tests for bridge_v2_catalog hook methods and the full bridge on BaseAdapter."""

from types import SimpleNamespace
from unittest import mock

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.catalogs import CatalogWriteIntegrationConfig


def _v2_catalog(name, catalog_type, table_format_value, config=None):
    return SimpleNamespace(
        name=name,
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
        config=config or {},
    )


class _StubAdapter:
    """Exposes BaseAdapter hook methods without requiring full adapter init."""

    type = mock.MagicMock(return_value="stub")
    _v2_to_v1_type = BaseAdapter._v2_to_v1_type
    _v2_table_format = BaseAdapter._v2_table_format
    _translate_v2_properties = BaseAdapter._translate_v2_properties
    bridge_v2_catalog = BaseAdapter.bridge_v2_catalog


class TestBaseAdapterV2Hooks:
    def setup_method(self):
        self.adapter = _StubAdapter()

    def test_v2_to_v1_type_passthrough(self):
        assert self.adapter._v2_to_v1_type("horizon") == "horizon"
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"

    def test_v2_table_format_returns_value(self):
        assert self.adapter._v2_table_format(_v2_catalog("cat", "horizon", "iceberg")) == "iceberg"
        assert (
            self.adapter._v2_table_format(_v2_catalog("cat", "hive_metastore", "default"))
            == "default"
        )

    def test_translate_v2_properties_passthrough(self):
        props = {"foo": "bar", "baz": 1}
        assert self.adapter._translate_v2_properties("any_type", props) == props
        assert self.adapter._translate_v2_properties("horizon", {}) == {}


class TestBaseAdapterBridgeV2Catalog:
    def setup_method(self):
        self.adapter = _StubAdapter()

    def test_returns_catalog_write_config(self):
        catalog = _v2_catalog("cat", "horizon", "iceberg")
        result = self.adapter.bridge_v2_catalog(catalog)
        assert isinstance(result, CatalogWriteIntegrationConfig)

    def test_basic_fields_extracted(self):
        catalog = _v2_catalog(
            "my_cat",
            "horizon",
            "iceberg",
            config={"stub": {"external_volume": "vol", "change_tracking": True}},
        )
        result = self.adapter.bridge_v2_catalog(catalog)
        assert result.name == "my_cat"
        assert result.catalog_name == "my_cat"
        assert result.catalog_type == "horizon"
        assert result.table_format == "iceberg"
        assert result.external_volume == "vol"
        assert result.adapter_properties == {"change_tracking": True}

    def test_file_format_extracted(self):
        catalog = _v2_catalog(
            "cat",
            "hive_metastore",
            "default",
            config={"stub": {"file_format": "delta"}},
        )
        result = self.adapter.bridge_v2_catalog(catalog)
        assert result.file_format == "delta"
        assert result.adapter_properties == {}

    def test_empty_platform_block(self):
        catalog = _v2_catalog("cat", "horizon", "iceberg")
        result = self.adapter.bridge_v2_catalog(catalog)
        assert result.external_volume is None
        assert result.file_format is None
        assert result.adapter_properties == {}

    def test_uses_self_type_as_platform_key(self):
        catalog = _v2_catalog(
            "cat",
            "horizon",
            "iceberg",
            config={"stub": {"external_volume": "correct"}, "other": {"external_volume": "wrong"}},
        )
        result = self.adapter.bridge_v2_catalog(catalog)
        assert result.external_volume == "correct"
