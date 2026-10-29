"""ADBC hands dbt a pyarrow DataType where PEP-249 promises an integer code.

Without the mapping under test, ``dbt show`` and model contracts label every column with its
arrow name (``int64``, ``string``) instead of a database type name.
"""

import datetime
from decimal import Decimal

import pytest

from dbt.adapters.lakecompute.type_mapping import data_type_code_to_name, normalize_value


class TestScalarTypes:
    @pytest.mark.parametrize(
        "arrow,expected",
        [
            ("bool", "BOOLEAN"),
            ("int8", "TINYINT"),
            ("int16", "SMALLINT"),
            ("int32", "INTEGER"),
            ("int64", "BIGINT"),
            ("uint64", "UBIGINT"),
            ("float", "FLOAT"),
            ("double", "DOUBLE"),
            ("string", "VARCHAR"),
            ("large_string", "VARCHAR"),
            ("binary", "BLOB"),
            ("date32[day]", "DATE"),
            ("null", "NULL"),
        ],
    )
    def test_known_types(self, arrow, expected):
        assert data_type_code_to_name(arrow) == expected

    def test_case_is_ignored(self):
        assert data_type_code_to_name("INT64") == "BIGINT"


class TestParameterisedTypes:
    def test_decimal_keeps_precision_and_scale(self):
        assert data_type_code_to_name("decimal128(18, 3)") == "DECIMAL(18,3)"
        assert data_type_code_to_name("decimal256(38, 9)") == "DECIMAL(38,9)"

    def test_bare_decimal(self):
        assert data_type_code_to_name("decimal") == "DECIMAL"

    def test_timestamp_without_timezone(self):
        assert data_type_code_to_name("timestamp[us]") == "TIMESTAMP"

    def test_timestamp_with_timezone(self):
        assert data_type_code_to_name("timestamp[ns, tz=UTC]") == "TIMESTAMP WITH TIME ZONE"

    @pytest.mark.parametrize(
        "arrow,expected",
        [
            ("time64[us]", "TIME"),
            ("duration[s]", "INTERVAL"),
            ("list<item: int64>", "LIST"),
            ("struct<a: int64>", "STRUCT"),
            ("map<string, int64>", "MAP"),
            ("dictionary<values=string, indices=int32>", "VARCHAR"),
        ],
    )
    def test_nested_and_temporal(self, arrow, expected):
        assert data_type_code_to_name(arrow) == expected


def test_unknown_type_does_not_raise():
    # a wrong label in `dbt show` is cosmetic; an exception there is not
    assert data_type_code_to_name("some_future_type") == "SOME_FUTURE_TYPE"


def test_accepts_a_real_pyarrow_type():
    pa = pytest.importorskip("pyarrow")
    assert data_type_code_to_name(pa.int64()) == "BIGINT"
    assert data_type_code_to_name(pa.decimal128(10, 2)) == "DECIMAL(10,2)"
    assert data_type_code_to_name(pa.timestamp("us", tz="UTC")) == "TIMESTAMP WITH TIME ZONE"


class TestNormalizeValue:
    def test_bytes_become_hex(self):
        # ForgivingJSONEncoder may reject raw bytes
        assert normalize_value(b"\x00\xff") == "00ff"
        assert normalize_value(bytearray(b"\x01")) == "01"
        assert normalize_value(memoryview(b"\x02")) == "02"

    @pytest.mark.parametrize("value", [None, "text", True, 1, 1.5, Decimal("1.5")])
    def test_scalars_pass_through(self, value):
        assert normalize_value(value) is value or normalize_value(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            datetime.date(2024, 1, 2),
            datetime.datetime(2024, 1, 2, 3, 4, 5),
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        ],
    )
    def test_dates_and_timestamps_pass_through(self, value):
        # `source freshness` (`_utc`) needs an object with tzinfo, not its str()
        assert normalize_value(value) is value

    @pytest.mark.parametrize("value", [datetime.time(1, 2, 3), datetime.timedelta(days=1)])
    def test_time_and_timedelta_are_stringified(self, value):
        # agate has no type for these
        assert normalize_value(value) == str(value)

    def test_dates_infer_agate_date_types(self):
        agate = pytest.importorskip("agate")
        from dbt_common.clients.agate_helper import table_from_data_flat

        rows = [
            normalize_value(v)
            for v in (
                datetime.date(2024, 1, 2),
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            )
        ]
        table = table_from_data_flat([{"d": rows[0], "ts": rows[1]}], ["d", "ts"])
        assert isinstance(table.column_types[0], agate.Date)
        assert isinstance(table.column_types[1], agate.DateTime)

    def test_containers_pass_through_for_agate_to_encode(self):
        assert normalize_value({"a": 1}) == {"a": 1}
        assert normalize_value([1, 2]) == [1, 2]

    def test_exotic_values_are_stringified(self):
        class Opaque:
            def __str__(self):
                return "opaque"

        assert normalize_value(Opaque()) == "opaque"
