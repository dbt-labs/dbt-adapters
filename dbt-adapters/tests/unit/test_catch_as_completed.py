from concurrent.futures import Future

import agate
from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER

from dbt.adapters.base.impl import catch_as_completed


def _make_catalog_table(rows):
    """Build an agate table mimicking catalog output."""
    return agate.Table.from_object(rows, column_types=DEFAULT_TYPE_TESTER)


def _resolved_future(value):
    """Create a Future that is already resolved with the given value."""
    f = Future()
    f.set_result(value)
    return f


def _failed_future(exc):
    """Create a Future that is already resolved with an exception."""
    f = Future()
    f.set_exception(exc)
    return f


class TestCatchAsCompleted:
    def test_empty_table_excluded_from_merge(self):
        """An empty catalog result should not cause a type conflict when merged
        with a non-empty result. This is the scenario from issue #1833: one schema
        returns rows (column_name inferred as Text) and another returns zero rows
        (column_name inferred as Number by agate default)."""
        non_empty = _make_catalog_table([{"column_name": "id", "column_type": "integer"}])
        empty = _make_catalog_table([])

        futures = [_resolved_future(non_empty), _resolved_future(empty)]
        result, exceptions = catch_as_completed(futures)

        assert len(exceptions) == 0
        assert len(result) == 1
        assert result[0]["column_name"] == "id"

    def test_all_empty_tables(self):
        """When every schema returns empty results, merge should still succeed."""
        empty1 = _make_catalog_table([])
        empty2 = _make_catalog_table([])

        futures = [_resolved_future(empty1), _resolved_future(empty2)]
        result, exceptions = catch_as_completed(futures)

        assert len(exceptions) == 0
        assert len(result) == 0

    def test_no_futures(self):
        """No futures at all should return an empty table."""
        result, exceptions = catch_as_completed([])

        assert len(exceptions) == 0
        assert len(result) == 0

    def test_multiple_non_empty_tables_merged(self):
        """Non-empty tables should still merge normally."""
        table1 = _make_catalog_table([{"column_name": "id", "column_type": "integer"}])
        table2 = _make_catalog_table([{"column_name": "name", "column_type": "text"}])

        futures = [_resolved_future(table1), _resolved_future(table2)]
        result, exceptions = catch_as_completed(futures)

        assert len(exceptions) == 0
        assert len(result) == 2

    def test_exception_collected(self):
        """Futures that raise exceptions should be collected, not crash."""
        non_empty = _make_catalog_table([{"column_name": "id", "column_type": "integer"}])
        futures = [
            _resolved_future(non_empty),
            _failed_future(RuntimeError("connection failed")),
        ]
        result, exceptions = catch_as_completed(futures)

        assert len(exceptions) == 1
        assert "connection failed" in str(exceptions[0])
        assert len(result) == 1
