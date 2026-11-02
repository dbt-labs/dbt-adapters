"""Unit tests for BigQueryAdapter bridge_v2_catalog hook methods."""

from types import SimpleNamespace

from dbt.adapters.bigquery.impl import BigQueryAdapter


def _v2_catalog(name, catalog_type, table_format_value, config=None):
    return SimpleNamespace(
        name=name,
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
        config=config or {},
    )


class TestBigQueryV2ToV1Type:
    def setup_method(self):
        self.adapter = object.__new__(BigQueryAdapter)

    def test_biglake_metastore(self):
        assert self.adapter._v2_to_v1_type("biglake_metastore") == "biglake_metastore"

    def test_unknown_passthrough(self):
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"
