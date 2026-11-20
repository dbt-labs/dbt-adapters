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

from dbt.adapters.sql import SQLAdapter
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


class TestGetColumnsInRelationFallback:
    """When the catalog result falls back to the driver, and when it must not."""

    def _adapter(self, datasharing=False):
        # Bypass __init__: the override needs only a real RedshiftAdapter for zero-arg super().
        adapter = RedshiftAdapter.__new__(RedshiftAdapter)
        adapter.get_columns_in_temp_relation = mock.Mock(return_value=["from_driver"])
        adapter.use_show_apis = mock.Mock(return_value=datasharing)
        return adapter

    def test_marked_temp_relation_skips_the_catalog_when_datasharing_is_on(self):
        # The whole point of the marker: the two catalog queries cost ~90s per temp relation
        # on a datashare consumer and cannot return rows there, so they must not run at all.
        adapter = self._adapter(datasharing=True)
        relation = mock.Mock(
            database=None, schema=None, identifier="model__dbt_tmp123", is_temporary=True
        )

        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", return_value=["from_catalog"]
        ) as from_catalog:
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_driver"]

        from_catalog.assert_not_called()
        adapter.get_columns_in_temp_relation.assert_called_once_with(relation)

    def test_marked_relation_that_is_qualified_does_not_take_the_driver_path(self):
        # get_columns_in_temp_relation queries `select * from <identifier>` with no database
        # or schema, so a qualified relation would resolve through search_path and describe
        # whatever else answers to that identifier -- quietly, since it would return columns.
        # No producer of a marked relation qualifies it today; this pins the precondition so
        # that stays true, and matches the check the empty-result fallback already makes.
        adapter = self._adapter(datasharing=True)
        relation = mock.Mock(
            database="db", schema="sch", identifier="model__dbt_tmp123", is_temporary=True
        )

        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", return_value=["from_catalog"]
        ) as from_catalog:
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_catalog"]

        from_catalog.assert_called_once()
        adapter.get_columns_in_temp_relation.assert_not_called()

    def test_marked_temp_relation_still_uses_the_catalog_when_datasharing_is_off(self):
        # Without datasharing the catalog can see temp relations, so the first query returns
        # and the expensive late-binding lookup is never reached -- there is nothing to win,
        # and the driver's type names are only ground truth for the OIDs it maps. Keeping the
        # catalog here is what confines this change to datashare consumers.
        adapter = self._adapter(datasharing=False)
        relation = mock.Mock(
            database=None, schema=None, identifier="model__dbt_tmp123", is_temporary=True
        )

        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", return_value=["from_catalog"]
        ) as from_catalog:
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_catalog"]

        from_catalog.assert_called_once()
        adapter.get_columns_in_temp_relation.assert_not_called()

    def test_marked_temp_relation_invisible_without_datasharing_still_falls_back(self):
        # A consumer database reached without `datasharing` set: the marker is not acted on,
        # so the empty-result net is the only thing standing between the user and
        # sync_all_columns dropping every target column. It has to still fire.
        adapter = self._adapter(datasharing=False)
        relation = mock.Mock(
            database=None, schema=None, identifier="model__dbt_tmp123", is_temporary=True
        )

        with mock.patch.object(SQLAdapter, "get_columns_in_relation", return_value=[]):
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_driver"]

        adapter.get_columns_in_temp_relation.assert_called_once_with(relation)

    def test_relation_without_the_marker_still_uses_the_catalog(self):
        # Relations reach this method from dbt-core and from other adapters' Relation classes
        # too, so a missing attribute has to read as "not a temp relation" rather than raise.
        adapter = self._adapter(datasharing=True)
        relation = mock.Mock(spec=["database", "schema", "identifier"])
        relation.database = "db"
        relation.schema = "sch"
        relation.identifier = "my_model"

        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", return_value=["from_catalog"]
        ):
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_catalog"]

        adapter.get_columns_in_temp_relation.assert_not_called()

    def test_catalog_result_is_returned_without_touching_the_driver(self):
        adapter = self._adapter()
        relation = mock.Mock(
            database=None, schema=None, identifier="model__dbt_tmp123", is_temporary=False
        )

        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", return_value=["from_catalog"]
        ):
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_catalog"]

        adapter.get_columns_in_temp_relation.assert_not_called()

    def test_invisible_temp_relation_falls_back_to_the_driver(self):
        # Unmarked but unqualified: the pre-existing net, for callers other than
        # redshift__make_temp_relation. Describe from the catalog first, driver only if empty.
        adapter = self._adapter()
        relation = mock.Mock(
            database=None, schema=None, identifier="model__dbt_tmp123", is_temporary=False
        )

        with mock.patch.object(SQLAdapter, "get_columns_in_relation", return_value=[]):
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == ["from_driver"]

        adapter.get_columns_in_temp_relation.assert_called_once_with(relation)

    def test_qualified_relation_with_no_columns_does_not_fall_back(self):
        # The driver query is unqualified, so it would resolve to the wrong relation.
        adapter = self._adapter()
        relation = mock.Mock(
            database="db", schema="sch", identifier="my_model", is_temporary=False
        )

        with mock.patch.object(SQLAdapter, "get_columns_in_relation", return_value=[]):
            assert RedshiftAdapter.get_columns_in_relation(adapter, relation) == []

        adapter.get_columns_in_temp_relation.assert_not_called()

    def test_driver_fallback_is_not_exposed_to_jinja(self):
        # @available here would put a method with no record type back in the Jinja context.
        assert "get_columns_in_temp_relation" not in RedshiftAdapter._available_
        assert "get_columns_in_relation" in RedshiftAdapter._available_


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
