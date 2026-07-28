"""
Unit tests for describing temporary relations from the driver's cursor description.

Needed when the connection's database is a datashare consumer database, where temporary
relations are invisible to information_schema.columns, pg_attribute and svv_columns
(dbt-labs/dbt-adapters#1947, #1991).

The expected data type names below are ground truth captured from
`SHOW COLUMNS FROM TABLE` on Redshift 1.0.358853. They must match exactly, because
incremental schema comparison diffs temp-relation columns (described here) against
target-relation columns (described by SHOW COLUMNS); any mismatch produces a spurious
type change on every run.
"""

from unittest import mock

import pytest

from dbt.adapters.redshift.impl import TYPE_OID_TO_DATA_TYPE, RedshiftAdapter

# (oid, typname, data type name reported by SHOW COLUMNS)
VERIFIED_TYPES = [
    (16, "bool", "boolean"),
    (20, "int8", "bigint"),
    (21, "int2", "smallint"),
    (23, "int4", "integer"),
    (25, "text", "character varying"),
    (700, "float4", "real"),
    (701, "float8", "double precision"),
    (1042, "bpchar", "character"),
    (1043, "varchar", "character varying"),
    (1082, "date", "date"),
    (1083, "time", "time without time zone"),
    (1114, "timestamp", "timestamp without time zone"),
    (1184, "timestamptz", "timestamp with time zone"),
    (1188, "intervaly2m", "interval year to month"),
    (1190, "intervald2s", "interval day to second"),
    (1266, "timetz", "time with time zone"),
    (1700, "numeric", "numeric"),
    (2935, "hllsketch", "hllsketch"),
    (3000, "geometry", "geometry"),
    (3001, "geography", "geography"),
    (4000, "super", "super"),
    (6551, "varbyte", "binary varying"),
]


class TestTypeOidMapping:
    @pytest.mark.parametrize("oid,typname,expected", VERIFIED_TYPES)
    def test_oid_maps_to_show_columns_data_type(self, oid, typname, expected):
        assert (
            TYPE_OID_TO_DATA_TYPE[oid] == expected
        ), f"OID {oid} ({typname}) must map to the name SHOW COLUMNS reports"

    def test_bpchar_is_character_not_character_varying(self):
        # char(n) and varchar(n) both satisfy Column.is_string(), so confusing them yields
        # 'character varying(n)' vs 'character(n)' and a type change that never converges.
        assert TYPE_OID_TO_DATA_TYPE[1042] == "character"
        assert TYPE_OID_TO_DATA_TYPE[1043] == "character varying"

    def test_varbyte_is_binary_varying(self):
        # SHOW COLUMNS reports 'binary varying', not 'varbyte'.
        assert TYPE_OID_TO_DATA_TYPE[6551] == "binary varying"


class _StubConnections:
    def __init__(self, cursor):
        self._cursor = cursor

    def add_select_query(self, sql):
        self.last_sql = sql
        return None, self._cursor

    @staticmethod
    def data_type_code_to_name(type_code):
        return "UNKNOWN"


class _StubAdapter:
    """Minimal stand-in exposing only what get_columns_in_temp_relation touches."""

    Column = None  # set below to _FakeColumn

    def __init__(self, description):
        cursor = mock.Mock()
        cursor.description = description
        self.connections = _StubConnections(cursor)

    @staticmethod
    def quote(identifier):
        return f'"{identifier}"'

    _temp_relation_data_type = RedshiftAdapter._temp_relation_data_type


class TestGetColumnsInTempRelation:
    def _adapter_with_description(self, description):
        adapter = _StubAdapter(description)
        adapter.Column = _FakeColumn
        return adapter

    def test_describes_columns_with_sizes_only_where_they_matter(self):
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        description = [
            ("id", 23, None, None, 10, 0, True),  # int4  -> integer
            ("name", 1043, None, None, 256, 0, True),  # varchar -> character varying(256)
            ("code", 1042, None, None, 10, 0, True),  # bpchar  -> character(10)
            ("amount", 1700, None, None, 18, 4, True),  # numeric -> numeric(18,4)
            ("ratio", 701, None, None, 17, 17, True),  # float8  -> double precision
            ("payload", 6551, None, None, 50, 0, True),  # varbyte -> binary varying
        ]
        adapter = self._adapter_with_description(description)

        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )

        assert [(c.column, c.dtype) for c in columns] == [
            ("id", "integer"),
            ("name", "character varying"),
            ("code", "character"),
            ("amount", "numeric"),
            ("ratio", "double precision"),
            ("payload", "binary varying"),
        ]

        by_name = {c.column: c for c in columns}
        # string types carry char_size
        assert by_name["name"].char_size == 256
        assert by_name["code"].char_size == 10
        # exact numerics carry precision and scale
        assert by_name["amount"].numeric_precision == 18
        assert by_name["amount"].numeric_scale == 4
        # everything else leaves sizes unset -- the driver reports display widths for these
        # (int4 -> 10, float8 -> 17/17) which are not the values SHOW COLUMNS reports
        assert by_name["id"].numeric_precision is None
        assert by_name["ratio"].numeric_precision is None
        assert by_name["ratio"].numeric_scale is None
        assert by_name["payload"].char_size is None

    def test_falls_back_to_driver_label_for_unknown_type_code(self):
        adapter = self._adapter_with_description([("mystery", 999999, None, None, 0, 0, True)])
        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )
        assert [c.dtype for c in columns] == ["unknown"]

    def test_empty_description_yields_no_columns(self):
        adapter = self._adapter_with_description(None)
        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )
        assert columns == []


class _FakeColumn:
    def __init__(self, column, dtype, char_size=None, numeric_precision=None, numeric_scale=None):
        self.column = column
        self.dtype = dtype
        self.char_size = char_size
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale
