import agate
from dbt_common.clients import agate_helper

from dbt.adapters.redshift import RedshiftAdapter
from tests.unit.utils import TestAdapterConversions


class TestConversion(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "a1", "stringval1"],
            ["", "a2", "stringvalasdfasdfasdfa"],
            ["", "a3", "stringval3"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["varchar(64)", "varchar(2)", "varchar(22)"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["integer", "float8", "integer"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ["", "false", "true"],
            ["", "false", "false"],
            ["", "false", "true"],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["boolean", "boolean", "boolean"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ["", "20190101T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190102T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190103T01:01:01Z", "2019-01-01 01:01:01"],
        ]
        agate_table = self._make_table_of(
            rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
        )
        expected = [
            "timestamp without time zone",
            "timestamp without time zone",
            "timestamp without time zone",
        ]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ["", "2019-01-01", "2019-01-04"],
            ["", "2019-01-02", "2019-01-04"],
            ["", "2019-01-03", "2019-01-04"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["date", "date", "date"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_date_type(agate_table, col_idx) == expect

    def test_convert_time_type(self):
        # dbt's default type testers actually don't have a TimeDelta at all.
        rows = [
            ["", "120s", "10s"],
            ["", "3m", "11s"],
            ["", "1h", "12s"],
        ]
        agate_table = self._make_table_of(rows, agate.TimeDelta)
        expected = ["varchar(24)", "varchar(24)", "varchar(24)"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_time_type(agate_table, col_idx) == expect
