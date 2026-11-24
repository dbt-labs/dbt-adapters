"""
Column names that dbt discovers in the warehouse are rendered into DDL/DML when it
evolves a schema (incremental `on_schema_change`, snapshots). A name containing the
quote character must stay a single identifier: the embedded quote is escaped, so the
rest of the name is never parsed as SQL.
"""

import pytest

from dbt.tests.util import run_dbt


MARKER_TABLE = "dbt_quoting_marker"
# Unescaped, the embedded quote would close the identifier and run a second statement.
QUOTE_COLUMN = f'a" text; create table {MARKER_TABLE} (i int); --'


def _quote(identifier: str) -> str:
    return '"{}"'.format(identifier.replace('"', '""'))


sources_yml = """
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: raw_orders
"""

incremental_sql = """
{{ config(materialized='incremental', unique_key='id', on_schema_change='ON_SCHEMA_CHANGE') }}

select * from {{ source('raw', 'raw_orders') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
"""

snapshot_sql = """
{% snapshot orders_snapshot %}
{{ config(target_schema=target.schema, unique_key='id', strategy='check', check_cols='all') }}
select * from {{ source('raw', 'raw_orders') }}
{% endsnapshot %}
"""


class BaseQuoteInColumnName:
    @pytest.fixture(scope="class", autouse=True)
    def raw_orders(self, project):
        project.run_sql(f"drop table if exists {MARKER_TABLE}")
        project.run_sql(
            "create table {schema}.raw_orders (id int, amt int);"
            "insert into {schema}.raw_orders values (1, 100);"
        )
        yield
        project.run_sql(f"drop table if exists {MARKER_TABLE}")

    @staticmethod
    def add_quote_column(project):
        project.run_sql(
            f"alter table {{schema}}.raw_orders add column {_quote(QUOTE_COLUMN)} text;"
            "insert into {schema}.raw_orders values (2, 200, 'x');"
        )

    @staticmethod
    def column_names(project, table):
        rows = project.run_sql(
            "select column_name from information_schema.columns"
            f" where table_schema = '{{schema}}' and table_name = '{table}'"
            " order by ordinal_position",
            fetch="all",
        )
        return [row[0] for row in rows]

    @staticmethod
    def assert_no_injected_table(project):
        rows = project.run_sql(
            f"select count(*) from pg_tables where tablename = '{MARKER_TABLE}'",
            fetch="one",
        )
        assert rows[0] == 0


class TestQuoteInColumnNameAppendNewColumns(BaseQuoteInColumnName):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": sources_yml,
            "orders_inc.sql": incremental_sql.replace("ON_SCHEMA_CHANGE", "append_new_columns"),
        }

    def test_new_column_with_quote_is_added(self, project):
        run_dbt(["run"])
        self.add_quote_column(project)
        run_dbt(["run"])

        self.assert_no_injected_table(project)
        assert QUOTE_COLUMN in self.column_names(project, "orders_inc")
        rows = project.run_sql(
            f"select id, {_quote(QUOTE_COLUMN)} from {{schema}}.orders_inc order by id",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [(1, None), (2, "x")]


class TestQuoteInColumnNameSyncAllColumns(BaseQuoteInColumnName):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": sources_yml,
            "orders_inc.sql": incremental_sql.replace("ON_SCHEMA_CHANGE", "sync_all_columns"),
        }

    def test_column_with_quote_is_added_then_dropped(self, project):
        run_dbt(["run"])
        self.add_quote_column(project)
        run_dbt(["run"])
        assert QUOTE_COLUMN in self.column_names(project, "orders_inc")

        project.run_sql(
            f"alter table {{schema}}.raw_orders drop column {_quote(QUOTE_COLUMN)};"
            "insert into {schema}.raw_orders values (3, 300);"
        )
        run_dbt(["run"])

        self.assert_no_injected_table(project)
        assert self.column_names(project, "orders_inc") == ["id", "amt"]
        rows = project.run_sql("select count(*) from {schema}.orders_inc", fetch="one")
        assert rows[0] == 3


class TestQuoteInColumnNameSnapshot(BaseQuoteInColumnName):
    @pytest.fixture(scope="class")
    def models(self):
        return {"sources.yml": sources_yml}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"orders_snapshot.sql": snapshot_sql}

    def test_new_column_with_quote_is_added(self, project):
        run_dbt(["snapshot"])
        self.add_quote_column(project)
        run_dbt(["snapshot"])

        self.assert_no_injected_table(project)
        assert QUOTE_COLUMN in self.column_names(project, "orders_snapshot")
        rows = project.run_sql(
            f"select {_quote(QUOTE_COLUMN)} from {{schema}}.orders_snapshot"
            " where dbt_valid_to is null order by id",
            fetch="all",
        )
        assert [row[0] for row in rows] == [None, "x"]
