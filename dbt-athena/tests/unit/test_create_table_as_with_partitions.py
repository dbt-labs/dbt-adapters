"""
Unit tests for the create_table_as_with_partitions macro.

Tests the full macro end-to-end using jinja2.FileSystemLoader with stubbed
dbt context, following the pattern used in test_get_partition_batches.py.
"""

import os
from unittest import mock

import jinja2
import pytest

_TABLE_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "materializations",
        "models",
        "table",
    )
)


class MockRelation:
    def __init__(self, identifier, schema="test_schema", database="awsdatacatalog"):
        self.identifier = identifier
        self.schema = schema
        self.database = database
        self.s3_path_table_part = identifier

    def __str__(self):
        return f"{self.database}.{self.schema}.{self.identifier}"


class MockColumn:
    def __init__(self, name):
        self.name = name
        self.quoted = f'"{name}"'


def _render_macro(batches, columns=None, temporary=False):
    """Render create_table_as_with_partitions with stubbed context.

    Args:
        batches: List of batch WHERE-clause strings returned by get_partition_batches.
                 Pass an empty list to simulate a zero-row source query.
        columns: List of MockColumn objects for adapter.get_columns_in_relation.
        temporary: Value for the ``temporary`` parameter.

    Returns:
        List of SQL strings passed to run_query, in call order.
    """
    if columns is None:
        columns = [MockColumn("date_col"), MockColumn("value")]

    relation = MockRelation("my_table")
    tmp_relation = MockRelation("my_table__tmp_not_partitioned")

    run_query_calls = []

    def mock_dispatch(macro_name, package_name=None):
        """Return a simple CREATE TABLE stub, bypassing athena__create_table_as."""
        def simple_create(tmp, rel, sql, lang="sql", skip=False):
            return f"CREATE TABLE {rel} AS {sql}"
        return simple_create

    def mock_run_query(sql):
        run_query_calls.append(str(sql).strip())
        return ""

    adapter = mock.Mock()
    adapter.dispatch = mock_dispatch
    adapter.get_columns_in_relation = mock.Mock(return_value=columns)

    api = mock.Mock()
    api.Relation.create = mock.Mock(return_value=tmp_relation)

    context = {
        "api": api,
        "adapter": adapter,
        "config": mock.Mock(),
        "target": mock.Mock(),
        "log": lambda *args, **kwargs: "",
        "run_query": mock_run_query,
        "drop_relation": lambda rel: "",
        "get_partition_batches": lambda **kwargs: batches,
        "exceptions": mock.Mock(),
    }
    context["config"].get = lambda key, *args, **kwargs: kwargs.get("default", args[0] if args else None)

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_TABLE_DIR),
        extensions=["jinja2.ext.do"],
    )

    template = env.get_template("create_table_as.sql", globals=context)
    template.module.create_table_as_with_partitions(temporary, relation, "SELECT * FROM source")

    return run_query_calls


class TestEmptyBatches:
    """When the source data is empty, get_partition_batches returns [].
    The macro must still create the target table so post-hooks don't fail."""

    def test_run_query_called_twice(self):
        """run_query is called once for staging CREATE and once for empty target CREATE."""
        calls = _render_macro(batches=[])
        assert len(calls) == 2

    def test_staging_table_created_first(self):
        """First run_query call creates the staging (tmp) table."""
        calls = _render_macro(batches=[])
        assert "my_table__tmp_not_partitioned" in calls[0]

    def test_target_table_created_from_staging(self):
        """Second run_query call creates the target table by selecting all rows from staging."""
        calls = _render_macro(batches=[])
        target_sql = calls[1]
        assert "my_table__tmp_not_partitioned" in target_sql
        # Empty CTAS: no WHERE clause
        assert "WHERE" not in target_sql.upper()

    def test_target_table_uses_correct_columns(self):
        """The empty CTAS selects the columns returned by get_columns_in_relation."""
        calls = _render_macro(batches=[], columns=[MockColumn("a"), MockColumn("b")])
        target_sql = calls[1]
        assert '"a"' in target_sql
        assert '"b"' in target_sql


class TestSingleBatch:
    """With one partition batch, the target table is created via CTAS with a WHERE clause."""

    def test_run_query_called_twice(self):
        calls = _render_macro(batches=['"date_col"=DATE\'2024-01-01\''])
        assert len(calls) == 2

    def test_target_table_uses_where_clause(self):
        batch = '"date_col"=DATE\'2024-01-01\''
        calls = _render_macro(batches=[batch])
        target_sql = calls[1]
        assert "WHERE" in target_sql.upper()
        assert batch in target_sql


class TestMultipleBatches:
    """With multiple batches, first creates via CTAS then inserts for subsequent batches."""

    def test_run_query_call_count(self):
        """N batches → 1 staging CREATE + 1 target CREATE + (N-1) INSERTs = N+1 calls."""
        batches = [
            '"date_col"=DATE\'2024-01-01\'',
            '"date_col"=DATE\'2024-01-02\'',
            '"date_col"=DATE\'2024-01-03\'',
        ]
        calls = _render_macro(batches=batches)
        assert len(calls) == len(batches) + 1

    def test_first_batch_creates_table(self):
        batches = ['"date_col"=DATE\'2024-01-01\'', '"date_col"=DATE\'2024-01-02\'']
        calls = _render_macro(batches=batches)
        # calls[0] = staging CREATE, calls[1] = first batch CREATE TABLE
        assert "CREATE TABLE" in calls[1].upper()

    def test_subsequent_batches_use_insert(self):
        batches = ['"date_col"=DATE\'2024-01-01\'', '"date_col"=DATE\'2024-01-02\'']
        calls = _render_macro(batches=batches)
        # calls[2] = second batch INSERT
        assert "INSERT INTO" in calls[2].upper()
