"""Can we connect, and can we run a query."""

import threading
from uuid import uuid4

import pytest

from dbt.adapters.contracts.connection import ConnectionState
from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtDatabaseError


class TestDebug:
    def test_debug_succeeds(self, project):
        run_dbt(["debug"])


class TestSelectOne:
    def test_execute_fetches_rows(self, project):
        results = project.run_sql("select 1 as id", fetch="all")
        assert len(results) == 1
        assert results[0][0] == 1

    def test_execute_without_fetch_returns_no_rows(self, project):
        # exercises the executemany(sql, None) path rather than execute()
        schema = f"{project.database}.{project.test_schema}"
        project.run_sql(f"create or replace table {schema}.no_fetch as select 1 as id", fetch=None)
        results = project.run_sql(f"select count(*) from {schema}.no_fetch", fetch="all")
        assert results[0][0] == 1

    def test_multi_statement_batch(self, project):
        """A `;`-joined batch goes out as one request and every statement in it runs."""
        schema = f"{project.database}.{project.test_schema}"
        project.run_sql(
            f"create or replace table {schema}.batch_a as select 1 as id;\n"
            f"create or replace table {schema}.batch_b as select 2 as id",
            fetch=None,
        )
        assert project.run_sql(f"select id from {schema}.batch_a", fetch="all")[0][0] == 1
        assert project.run_sql(f"select id from {schema}.batch_b", fetch="all")[0][0] == 2


class TestAdapterPlumbing:
    def test_adapter_type(self, project):
        assert project.adapter.type() == "lakecompute"

    def test_relations_are_unquoted(self, project):
        relation = project.adapter.Relation.create(database="db", schema="sch", identifier="tbl")
        assert relation.render() == "db.sch.tbl"
        assert relation.without_identifier().render() == "db.sch"

    def test_column_types_are_duckdb_names(self, project):
        """Guards the arrow -> DuckDB description mapping.

        Without it these come back as `int64` / `string`.
        """
        with project.adapter.connection_named("test"):
            columns = project.adapter.get_column_schema_from_query(
                "select 1::bigint as i, 'x' as s, 1.5::double as d"
            )
        assert [(column.name, column.dtype) for column in columns] == [
            ("i", "BIGINT"),
            ("s", "VARCHAR"),
            ("d", "DOUBLE"),
        ]

    def test_query_failure_allows_connection_cleanup(self, project):
        missing_table = f"{project.database}.{project.test_schema}.missing_{uuid4().hex}"
        with pytest.raises(DbtDatabaseError):
            with project.adapter.connection_named("failing_query"):
                connection = project.adapter.connections.get_thread_connection()
                project.adapter.connections.execute(f"select * from {missing_table}", fetch=True)

        assert connection.state == ConnectionState.CLOSED
        with project.adapter.connection_named("after_failure"):
            _, table = project.adapter.connections.execute("select 1 as id", fetch=True)
        assert table.rows[0][0] == 1

    def test_no_query_comment_is_prepended(self, project):
        """Off unless the profile opts in."""
        with project.adapter.connection_named("test"):
            commented = project.adapter.connections._add_query_comment("select 1")
        assert commented == "select 1"


class TestConcurrency:
    """Several threads share one AdbcDatabase; each gets its own AdbcConnection.

    ``_relations_cache_for_schemas`` fans out over a thread pool before any model runs, so
    this is the shape of the first concurrent access the registry sees.
    """

    def test_threads_each_get_a_connection_over_one_database(self, project):
        from dbt.adapters.lakecompute import connections as conn_module

        errors: list = []
        results: list = []

        def query(n: int) -> None:
            try:
                with project.adapter.connection_named(f"thread_{n}"):
                    _, table = project.adapter.connections.execute(f"select {n} as id", fetch=True)
                    results.append(table.rows[0][0])
            except Exception as exc:  # pragma: no cover - surfaced via the assert below
                errors.append(exc)

        threads = [threading.Thread(target=query, args=(n,)) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert sorted(results) == [0, 1, 2, 3]
        # one shared database for this target, not one per thread
        base_url = project.adapter.config.credentials.resolved_base_url
        # registry keys are (path, entrypoint, sorted db_kwargs)
        matching = [key for key in conn_module._REGISTRY if dict(key[2]).get("uri") == base_url]
        assert len(matching) == 1
