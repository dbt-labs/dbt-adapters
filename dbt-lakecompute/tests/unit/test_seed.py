"""Seed rows are rendered as literal SQL, since the driver has no parameter bindings."""

import datetime
from decimal import Decimal
from unittest import mock

import agate
import pytest

from dbt.adapters.lakecompute.impl import LakeComputeAdapter
from dbt.adapters.lakecompute.relation import LakeComputeRelation
from dbt.adapters.lakecompute.seed import insert_statements, render_literal


class TestRenderLiteral:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "NULL"),
            (True, "true"),
            (False, "false"),
            (Decimal("7"), "7"),
            (Decimal("1.50"), "1.50"),
            # fixed-point, never exponent notation
            (Decimal("1E+3"), "1000"),
            # bare NaN / Infinity would be column references
            (Decimal("NaN"), "'NaN'::double"),
            (Decimal("Infinity"), "'Infinity'::double"),
            (Decimal("-Infinity"), "'-Infinity'::double"),
            (datetime.date(2023, 1, 5), "'2023-01-05'"),
            (datetime.datetime(2023, 1, 5, 10, 30), "'2023-01-05T10:30:00'"),
            ("Alice", "'Alice'"),
            ("O'Brien", "'O''Brien'"),
            ("", "''"),
        ],
    )
    def test_literal(self, value, expected):
        assert render_literal(value) == expected


class TestInsertStatements:
    def test_one_statement_in_the_recorded_shape(self):
        rows = [(Decimal(1), "Alice", "US"), (Decimal(2), "Bob", None)]
        assert list(insert_statements("d.s.t", ["id", "name", "country"], rows)) == [
            "insert into d.s.t (id, name, country) values (1, 'Alice', 'US'), (2, 'Bob', NULL)"
        ]

    def test_no_rows_no_statements(self):
        assert list(insert_statements("d.s.t", ["id"], [])) == []

    def test_batches_by_row_count(self):
        rows = [(Decimal(i),) for i in range(5)]
        statements = list(insert_statements("t", ["id"], rows, max_rows=2))
        assert statements == [
            "insert into t (id) values (0), (1)",
            "insert into t (id) values (2), (3)",
            "insert into t (id) values (4)",
        ]

    def test_batches_by_bytes(self):
        rows = [("x" * 10,) for _ in range(4)]
        statements = list(insert_statements("t", ["c"], rows, max_bytes=60))
        assert all(len(s.encode()) <= 60 for s in statements)
        assert sum(s.count("'xxxxxxxxxx'") for s in statements) == 4
        assert len(statements) > 1

    def test_bytes_not_characters(self):
        # each "é" is two bytes, so both rows fit in 60 characters but not in 60 bytes
        rows = [("é" * 10,), ("é" * 10,)]
        together = "insert into t (c) values " + ", ".join(["('" + "é" * 10 + "')"] * 2)
        assert len(together) <= 60 < len(together.encode())
        assert len(list(insert_statements("t", ["c"], rows, max_bytes=60))) == 2

    def test_an_oversized_row_is_still_sent(self):
        rows = [("x" * 100,), ("y",)]
        statements = list(insert_statements("t", ["c"], rows, max_bytes=20))
        assert statements == [
            "insert into t (c) values ('" + "x" * 100 + "')",
            "insert into t (c) values ('y')",
        ]


class TestAdapterSeedMethods:
    @pytest.fixture
    def adapter(self):
        # nothing here touches instance state except add_query, which each test stubs
        return LakeComputeAdapter.__new__(LakeComputeAdapter)

    def test_quote_doubles_embedded_quotes(self):
        assert LakeComputeAdapter.quote('a"b') == '"a""b"'

    def test_load_seed_rows_closes_every_cursor(self, adapter):
        table = agate.Table(
            [(i, "a") for i in range(501)],
            ["id", "order"],
            [agate.Number(), agate.Text()],
        )
        relation = LakeComputeRelation.create(database="d", schema="s", identifier="t")
        cursors = []

        def add_query(sql, abridge_sql_log=False):
            assert abridge_sql_log
            cursors.append(mock.Mock())
            return None, cursors[-1]

        adapter.add_query = add_query
        first = adapter.load_seed_rows(relation, table, None)

        assert first.startswith("insert into d.s.t (\"id\", \"order\") values (0, 'a'), (1, 'a')")
        assert first.endswith("(499, 'a')")
        # 500 rows per statement
        assert len(cursors) == 2
        assert all(c.close.called for c in cursors)

    def test_load_seed_rows_of_no_rows(self, adapter):
        table = agate.Table([], ["id"], [agate.Number()])
        relation = LakeComputeRelation.create(database="d", schema="s", identifier="t")
        adapter.add_query = mock.Mock()
        assert adapter.load_seed_rows(relation, table, None) == ""
        adapter.add_query.assert_not_called()


class TestSeedColumnTypes:
    """DuckDB's `integer` is 32-bit, so whole numbers are typed `bigint`."""

    @pytest.mark.parametrize(
        "rows,expected",
        [
            ([(Decimal("1"),), (Decimal("3000000000"),)], "bigint"),
            ([(Decimal("1.5"),)], "double"),
            # NaN and Infinity have no precision, but only fit a double
            ([(Decimal("1"),), (Decimal("NaN"),), (None,)], "double"),
            ([(Decimal("-Infinity"),)], "double"),
        ],
    )
    def test_number(self, rows, expected):
        table = agate.Table(rows, ["n"], [agate.Number()])
        assert LakeComputeAdapter.convert_number_type(table, 0) == expected

    def test_integer(self):
        table = agate.Table([(1,)], ["n"], [agate.Number()])
        assert LakeComputeAdapter.convert_integer_type(table, 0) == "bigint"
