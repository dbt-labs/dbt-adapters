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

Size/precision/scale cannot come from cursor.description's PEP 249 fields: verified
directly against redshift_connector's source, those are hardcoded to None -- not merely
undocumented, dead on every version dbt-redshift depends on. The real values come from
cursor.ps["row_desc"]'s type_modifier (pg_attribute.atttypmod), which the driver parses
off the wire but never surfaces through the public API. The stub below mirrors that split
so these tests fail the same way the real driver would if the decode logic regresses.
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


def _varlen_modifier(length):
    """Postgres/Redshift wire-protocol atttypmod for a declared string length."""
    return length + 4


def _numeric_modifier(precision, scale):
    """Postgres/Redshift wire-protocol atttypmod for a declared numeric(precision, scale)."""
    return ((precision << 16) | scale) + 4


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
    """Minimal stand-in exposing only what get_columns_in_temp_relation touches.

    Mirrors the real ``redshift_connector`` cursor: ``description`` carries the PEP 249
    tuple with size/precision/scale hardcoded to ``None`` (verified against the driver's
    source -- these fields are never populated), and ``ps["row_desc"]`` carries the real
    per-column ``type_modifier`` the driver parses off the wire but doesn't surface
    through the public API.
    """

    Column = None  # set below to _FakeColumn

    def __init__(self, columns):
        # columns: list of (name, type_code, type_modifier_or_None)
        cursor = mock.Mock()
        cursor.description = (
            [(name, type_code, None, None, None, None, None) for name, type_code, _ in columns]
            if columns is not None
            else None
        )
        cursor.ps = {
            "row_desc": [
                {"type_modifier": modifier if modifier is not None else -1}
                for _, _, modifier in (columns or [])
            ]
        }
        self.connections = _StubConnections(cursor)

    @staticmethod
    def quote(identifier):
        return f'"{identifier}"'

    _temp_relation_data_type = RedshiftAdapter._temp_relation_data_type


class TestGetColumnsInTempRelation:
    def _adapter_with_columns(self, columns):
        adapter = _StubAdapter(columns)
        adapter.Column = _FakeColumn
        return adapter

    def test_describes_columns_with_sizes_only_where_they_matter(self):
        columns = [
            ("id", 23, None),  # int4  -> integer, no modifier
            ("name", 1043, _varlen_modifier(256)),  # varchar(256) -> character varying(256)
            ("code", 1042, _varlen_modifier(10)),  # bpchar(10)  -> character(10)
            ("amount", 1700, _numeric_modifier(18, 4)),  # numeric(18,4)
            ("ratio", 701, None),  # float8  -> double precision, no modifier
            ("payload", 6551, None),  # varbyte -> binary varying, no modifier
        ]
        adapter = self._adapter_with_columns(columns)

        result = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )

        assert [(c.column, c.dtype) for c in result] == [
            ("id", "integer"),
            ("name", "character varying"),
            ("code", "character"),
            ("amount", "numeric"),
            ("ratio", "double precision"),
            ("payload", "binary varying"),
        ]

        by_name = {c.column: c for c in result}
        # string types carry char_size, decoded from type_modifier
        assert by_name["name"].char_size == 256
        assert by_name["code"].char_size == 10
        # exact numerics carry precision and scale, decoded from type_modifier
        assert by_name["amount"].numeric_precision == 18
        assert by_name["amount"].numeric_scale == 4
        # everything else leaves sizes unset
        assert by_name["id"].numeric_precision is None
        assert by_name["ratio"].numeric_precision is None
        assert by_name["ratio"].numeric_scale is None
        assert by_name["payload"].char_size is None

    def test_unconstrained_string_and_numeric_leave_size_unset(self):
        # type_modifier == -1 means "no modifier" (unconstrained/default size).
        columns = [("name", 1043, -1), ("amount", 1700, -1)]
        adapter = self._adapter_with_columns(columns)

        result = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )

        by_name = {c.column: c for c in result}
        assert by_name["name"].char_size is None
        assert by_name["amount"].numeric_precision is None
        assert by_name["amount"].numeric_scale is None

    def test_falls_back_to_driver_label_for_unknown_type_code(self):
        adapter = self._adapter_with_columns([("mystery", 999999, None)])
        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )
        assert [c.dtype for c in columns] == ["unknown"]

    def test_empty_description_yields_no_columns(self):
        adapter = self._adapter_with_columns(None)
        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )
        assert columns == []

    def test_missing_row_desc_falls_back_to_no_size_info(self):
        # cursor.ps["row_desc"] is undocumented driver internals; if a future driver
        # version removes or restructures it, columns should still come back (without
        # sizes) rather than raising.
        adapter = self._adapter_with_columns([("amount", 1700, _numeric_modifier(18, 4))])
        adapter.connections._cursor.ps = {}

        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )

        assert columns[0].dtype == "numeric"
        assert columns[0].numeric_precision is None
        assert columns[0].numeric_scale is None


class _FakeColumn:
    def __init__(self, column, dtype, char_size=None, numeric_precision=None, numeric_scale=None):
        self.column = column
        self.dtype = dtype
        self.char_size = char_size
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale
