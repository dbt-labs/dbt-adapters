"""Unit tests for bridge_v2_catalog hook methods on BaseAdapter.

bridge_v2_catalog lazily imports CatalogWriteIntegrationConfig from dbt.artifacts
(dbt-core), which is not installed in this test environment. The full bridge is
tested end-to-end in dbt-snowflake functional tests. These tests cover the hook
methods that adapters override.
"""

from types import SimpleNamespace

from dbt.adapters.base.impl import BaseAdapter


def _v2_catalog(catalog_type, table_format_value):
    return SimpleNamespace(
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
    )


class _StubAdapter:
    """Exposes BaseAdapter hook methods without requiring full adapter init."""

    _v2_to_v1_type = BaseAdapter._v2_to_v1_type
    _v2_table_format = BaseAdapter._v2_table_format
    _translate_v2_properties = BaseAdapter._translate_v2_properties


class TestBaseAdapterV2Hooks:
    def setup_method(self):
        self.adapter = _StubAdapter()

    def test_v2_to_v1_type_passthrough(self):
        assert self.adapter._v2_to_v1_type("horizon") == "horizon"
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"

    def test_v2_table_format_returns_value(self):
        assert self.adapter._v2_table_format(_v2_catalog("horizon", "iceberg")) == "iceberg"
        assert self.adapter._v2_table_format(_v2_catalog("hive_metastore", "default")) == "default"

    def test_translate_v2_properties_passthrough(self):
        props = {"foo": "bar", "baz": 1}
        assert self.adapter._translate_v2_properties("any_type", props) == props
        assert self.adapter._translate_v2_properties("horizon", {}) == {}
