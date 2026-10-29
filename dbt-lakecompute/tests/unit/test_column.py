"""Columns keep the type names `describe` reports."""

import pytest

from dbt.adapters.lakecompute.column import LakeComputeColumn


class TestDataType:
    @pytest.mark.parametrize(
        "dtype",
        [
            "VARCHAR",
            "BIGINT",
            "DECIMAL(38,2)",
            "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE",
            "STRUCT(a INTEGER, b VARCHAR)",
            "INTEGER[]",
            "MAP(VARCHAR, INTEGER)",
            "BOOLEAN",
        ],
    )
    def test_data_type_is_returned_verbatim(self, dtype):
        assert LakeComputeColumn("c", dtype).data_type == dtype

    def test_expanded_data_type_matches_data_type(self):
        assert LakeComputeColumn("c", "DECIMAL(38,2)").expanded_data_type == "DECIMAL(38,2)"

    def test_base_class_would_have_mangled_a_varchar(self):
        from dbt.adapters.base.column import Column

        assert Column("c", "varchar").data_type == "character varying(256)"
        assert LakeComputeColumn("c", "varchar").data_type == "varchar"

    def test_numeric_precision_and_scale_are_ignored(self):
        column = LakeComputeColumn("c", "DECIMAL(38,2)", numeric_precision=38, numeric_scale=2)
        assert column.data_type == "DECIMAL(38,2)"


class TestTranslateType:
    def test_type_labels_are_empty(self):
        assert LakeComputeColumn.TYPE_LABELS == {}

    def test_translate_type_is_a_no_op(self):
        assert LakeComputeColumn.translate_type("STRING") == "STRING"

    def test_create_does_not_rewrite_the_type(self):
        assert LakeComputeColumn.create("c", "STRING").data_type == "STRING"


class TestQuoting:
    # Relations are unquoted (quote policy), but columns are quoted the same way as
    # `adapter.quote()`, so a reserved-word column name works on every code path.

    def test_quoted_matches_adapter_quote(self):
        from dbt.adapters.lakecompute.impl import LakeComputeAdapter

        column = LakeComputeColumn("order", "VARCHAR")
        assert column.quoted == '"order"'
        assert column.quoted == LakeComputeAdapter.quote("order")


class TestTypeClassification:
    @pytest.mark.parametrize("dtype", ["VARCHAR", "varchar", "TEXT", "STRING", "CHAR"])
    def test_string_types(self, dtype):
        assert LakeComputeColumn("c", dtype).is_string()

    @pytest.mark.parametrize("dtype", ["BIGINT", "HUGEINT", "UTINYINT", "INTEGER", "int4"])
    def test_integer_types(self, dtype):
        column = LakeComputeColumn("c", dtype)
        assert column.is_integer()
        assert column.is_number()

    @pytest.mark.parametrize("dtype", ["DOUBLE", "FLOAT", "REAL"])
    def test_float_types(self, dtype):
        assert LakeComputeColumn("c", dtype).is_float()

    @pytest.mark.parametrize("dtype", ["DECIMAL(38,2)", "NUMERIC", "decimal"])
    def test_numeric_types(self, dtype):
        assert LakeComputeColumn("c", dtype).is_numeric()

    @pytest.mark.parametrize("dtype", ["TIMESTAMP", "DATE", "BOOLEAN", "BLOB", "INTEGER[]"])
    def test_non_numeric_non_string_types(self, dtype):
        column = LakeComputeColumn("c", dtype)
        assert not column.is_string()
        assert not column.is_number()
