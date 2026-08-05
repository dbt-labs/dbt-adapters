"""
Unit tests for describing temporary relations from the driver's cursor description.

Needed when the connection's database is a datashare consumer database, where temporary
relations are invisible to information_schema.columns, pg_attribute and svv_columns
(dbt-labs/dbt-adapters#1947, #1991).

The expected data type names are ground truth captured from `SHOW COLUMNS FROM TABLE` on
Redshift 1.0.358853, and must match the catalog exactly or every column of that type reads as
changed on every run. tests/functional/test_type_oid_mapping.py checks that live.

The cursor stub mirrors the real driver: dead PEP 249 size fields on `description`, real
type_modifier on `ps["row_desc"]`, and a type-name lookup that raises for an unknown OID.
"""

from unittest import mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.redshift.impl import (
    TYPE_OID_TO_DATA_TYPE,
    TYPE_OID_TO_INFORMATION_SCHEMA_DATA_TYPE,
    RedshiftAdapter,
)

# (oid, typname, data type name reported by the catalog)
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
    def test_oid_maps_to_catalog_data_type(self, oid, typname, expected):
        assert (
            TYPE_OID_TO_DATA_TYPE[oid] == expected
        ), f"OID {oid} ({typname}) must map to the name the catalog reports"

    def test_bpchar_is_character_not_character_varying(self):
        # char(n) and varchar(n) both satisfy Column.is_string(), so confusing them yields
        # 'character varying(n)' vs 'character(n)' and a type change that never converges.
        assert TYPE_OID_TO_DATA_TYPE[1042] == "character"
        assert TYPE_OID_TO_DATA_TYPE[1043] == "character varying"

    def test_varbyte_is_binary_varying(self):
        # SHOW COLUMNS reports 'binary varying', not 'varbyte'.
        assert TYPE_OID_TO_DATA_TYPE[6551] == "binary varying"

    def test_information_schema_overrides_only_where_the_catalogs_differ(self):
        # SHOW COLUMNS gives the SQL name, information_schema its internal one. Only the two
        # interval types diverge; anything else here would be a silent behaviour change.
        assert TYPE_OID_TO_INFORMATION_SCHEMA_DATA_TYPE == {
            1188: "intervaly2m",
            1190: "intervald2s",
        }


def _varlen_modifier(length):
    """Postgres/Redshift wire-protocol atttypmod for a declared string length."""
    return length + 4


def _numeric_modifier(precision, scale):
    """Postgres/Redshift wire-protocol atttypmod for a declared numeric(precision, scale)."""
    return ((precision << 16) | scale) + 4


# OIDs redshift_connector names but this adapter does not map, and the labels it gives them.
DRIVER_ONLY_TYPES = {1015: "VARCHAR_ARRAY", 28: "XID"}


class _StubConnections:
    def __init__(self, cursor):
        self._cursor = cursor

    def add_select_query(self, sql):
        self.last_sql = sql
        return None, self._cursor

    @staticmethod
    def data_type_code_to_name(type_code):
        # The real implementation is `RedshiftOID(oid).name`, an IntEnum call, so it raises
        # rather than returning a label for an OID the driver doesn't know.
        if type_code not in DRIVER_ONLY_TYPES:
            raise ValueError(f"{type_code} is not a valid RedshiftOID")
        return DRIVER_ONLY_TYPES[type_code]


class _StubAdapter:
    """Minimal stand-in exposing only what get_columns_in_temp_relation touches."""

    Column = None  # set below to _FakeColumn
    datasharing = True  # SHOW COLUMNS names; flip to get information_schema names

    def use_show_apis(self):
        return self.datasharing

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

    def test_interval_follows_the_describer_the_target_used(self):
        # datasharing off means the target was described by information_schema, which names the
        # interval types differently -- following SHOW COLUMNS there is a type change per run.
        columns = [("span", 1188, -1)]

        adapter = self._adapter_with_columns(columns)
        relation = mock.Mock(identifier="model__dbt_tmp123")
        assert RedshiftAdapter.get_columns_in_temp_relation(adapter, relation)[0].dtype == (
            "interval year to month"
        )

        adapter = self._adapter_with_columns(columns)
        adapter.datasharing = False
        assert (
            RedshiftAdapter.get_columns_in_temp_relation(adapter, relation)[0].dtype
            == "intervaly2m"
        )

    def test_falls_back_to_driver_label_for_unmapped_type_code(self):
        # An OID this adapter doesn't map but the driver can still name.
        adapter = self._adapter_with_columns([("tags", 1015, None)])
        columns = RedshiftAdapter.get_columns_in_temp_relation(
            adapter, mock.Mock(identifier="model__dbt_tmp123")
        )
        assert [c.dtype for c in columns] == ["varchar_array"]

    def test_raises_for_type_code_neither_side_recognises(self):
        # The driver's lookup raises rather than yielding a label, and a column whose type
        # cannot be named at all would only fail later as invalid DDL -- so fail here, with
        # the offending type code in the message.
        adapter = self._adapter_with_columns([("mystery", 999999, None)])
        with pytest.raises(DbtRuntimeError, match="999999"):
            RedshiftAdapter.get_columns_in_temp_relation(
                adapter, mock.Mock(identifier="model__dbt_tmp123")
            )

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
