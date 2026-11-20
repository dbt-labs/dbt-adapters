"""Unit tests for SnowflakeAdapter bridge_v2_catalog hook methods."""

from types import SimpleNamespace

from dbt.adapters.snowflake.impl import SnowflakeAdapter


def _v2_catalog(name, catalog_type, table_format_value, config=None):
    return SimpleNamespace(
        name=name,
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
        config=config or {},
    )


class TestSnowflakeV2ToV1Type:
    def setup_method(self):
        self.adapter = object.__new__(SnowflakeAdapter)

    def test_horizon(self):
        assert self.adapter._v2_to_v1_type("horizon") == "BUILT_IN"

    def test_glue(self):
        assert self.adapter._v2_to_v1_type("glue") == "ICEBERG_REST"

    def test_iceberg_rest(self):
        assert self.adapter._v2_to_v1_type("iceberg_rest") == "ICEBERG_REST"

    def test_unity(self):
        assert self.adapter._v2_to_v1_type("unity") == "ICEBERG_REST"

    def test_unknown_passthrough(self):
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"


class TestSnowflakeV2TableFormat:
    def setup_method(self):
        self.adapter = object.__new__(SnowflakeAdapter)

    def test_iceberg_uppercased(self):
        catalog = _v2_catalog("cat", "horizon", "iceberg")
        assert self.adapter._v2_table_format(catalog) == "ICEBERG"

    def test_default_uppercased(self):
        catalog = _v2_catalog("cat", "hive_metastore", "default")
        assert self.adapter._v2_table_format(catalog) == "DEFAULT"


class TestSnowflakeTranslateV2Properties:
    def setup_method(self):
        self.adapter = object.__new__(SnowflakeAdapter)

    def test_horizon_no_translation(self):
        props = {"change_tracking": True, "base_location_root": "path"}
        result = self.adapter._translate_v2_properties("horizon", props)
        assert result == props

    def test_glue_translates_catalog_database(self):
        props = {"catalog_database": "MY_GLUE_DB", "auto_refresh": True}
        result = self.adapter._translate_v2_properties("glue", props)
        assert "catalog_linked_database" in result
        assert result["catalog_linked_database"] == "MY_GLUE_DB"
        assert "catalog_database" not in result
        assert result["catalog_linked_database_type"] == "glue"
        assert result["auto_refresh"] is True

    def test_unity_translates_catalog_database(self):
        props = {"catalog_database": "UNITY_DB"}
        result = self.adapter._translate_v2_properties("unity", props)
        assert result["catalog_linked_database"] == "UNITY_DB"
        assert result["catalog_linked_database_type"] == "unity"

    def test_iceberg_rest_translates_no_type_injected(self):
        props = {"catalog_database": "REST_DB"}
        result = self.adapter._translate_v2_properties("iceberg_rest", props)
        assert result["catalog_linked_database"] == "REST_DB"
        assert "catalog_linked_database_type" not in result

    def test_non_linked_type_no_translation(self):
        props = {"some_prop": "val"}
        result = self.adapter._translate_v2_properties("horizon", props)
        assert result == props
