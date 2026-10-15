"""
Regression tests for the empty-source-columns guard in default__process_schema_changes.

When column introspection of the incremental temp relation returns no rows, dbt used to treat
that as "the source has no columns" and, under on_schema_change='sync_all_columns', issue
`drop column` for every column in the target before failing on the resulting `insert into t ()`.
This is reachable in the wild: on Redshift, connecting to a datashare consumer database makes
temporary relations invisible to information_schema.columns, pg_attribute and svv_columns, while
the relation itself remains fully queryable (dbt-labs/dbt-adapters#1947, #1991).

The guard's behaviour depends on on_schema_change, because only some values are destructive:

  sync_all_columns   raises -- would otherwise drop every target column
  fail               raises -- would otherwise report a bogus "out of sync" diff
  append_new_columns warns  -- cannot drop anything, and the materialization substitutes the
                              target's columns for the empty return, so the run still works
  ignore             never reaches the guard (process_schema_changes returns early)

These tests simulate the condition adapter-agnostically by overriding
postgres__get_columns_in_relation to return an empty list for the temp relation only.
"""

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
import pytest

_MODEL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

with source_data as (
    select 1 as id, 'aaa' as field_1, 'bbb' as field_2
    union all select 2 as id, 'ccc' as field_1, 'ddd' as field_2
    union all select 3 as id, 'eee' as field_1, 'fff' as field_2
)

select * from source_data

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
"""


def _with_on_schema_change(value):
    return _MODEL_SYNC_ALL_COLUMNS.replace(
        "on_schema_change='sync_all_columns'", "on_schema_change='%s'" % value
    )


_MODEL_IGNORE = _with_on_schema_change("ignore")
_MODEL_APPEND_NEW_COLUMNS = _with_on_schema_change("append_new_columns")
_MODEL_FAIL = _with_on_schema_change("fail")


# Same model as _MODEL_APPEND_NEW_COLUMNS with a third column added, so the second run has a
# genuinely new column that append_new_columns would normally pick up.
_MODEL_APPEND_NEW_COLUMNS_DRIFTED = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

with source_data as (
    select 1 as id, 'aaa' as field_1, 'bbb' as field_2, 'ggg' as field_3
    union all select 2 as id, 'ccc' as field_1, 'ddd' as field_2, 'hhh' as field_3
    union all select 3 as id, 'eee' as field_1, 'fff' as field_2, 'iii' as field_3
)

select * from source_data

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
"""


# Returns an empty column list for the incremental temp relation only, reproducing a warehouse
# whose catalog cannot see it. All other relations introspect normally.
_MACRO_EMPTY_TEMP_COLUMNS = """
{% macro postgres__get_columns_in_relation(relation) -%}

  {% if '__dbt_tmp' in (relation.identifier or '') %}
    {{ return([]) }}
  {% endif %}

  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from {{ relation.information_schema('columns') }}
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}
"""


def _column_names(project, identifier):
    rows = project.run_sql(
        """
        select column_name
        from information_schema.columns
        where table_schema = '{schema}'
          and table_name = '"""
        + identifier
        + """'
        order by ordinal_position
        """,
        fetch="all",
    )
    return [row[0] for row in rows]


class _EmptyTempColumns:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"empty_temp_columns.sql": _MACRO_EMPTY_TEMP_COLUMNS}


class TestEmptySourceColumnsRaises(_EmptyTempColumns):
    """sync_all_columns must fail loudly rather than drop every target column."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_sync_all_columns.sql": _MODEL_SYNC_ALL_COLUMNS}

    def test_raises_before_issuing_any_drop(self, project):
        # First run builds the table normally; the temp-relation override is not exercised
        # because a non-incremental build has no temp relation to introspect.
        run_dbt(["run", "--select", "incremental_sync_all_columns"])
        assert _column_names(project, "incremental_sync_all_columns") == [
            "id",
            "field_1",
            "field_2",
        ]

        # Second run goes down the incremental path, where introspecting the temp relation
        # returns []. This must raise rather than proceed.
        results, logs = run_dbt_and_capture(
            ["--debug", "run", "--select", "incremental_sync_all_columns"], expect_pass=False
        )
        assert len(results) == 1
        assert "Could not read any columns" in results[0].message
        assert "metadata failure" in results[0].message

        # The load-bearing assertion: unguarded, dbt issues `drop column` for all three before
        # failing on `insert into ... ()`. Postgres rolls that back with the failed transaction,
        # so comparing column lists cannot tell the two runs apart -- on a warehouse that commits
        # the DDL the columns stay gone.
        assert "drop column" not in logs

        assert _column_names(project, "incremental_sync_all_columns") == [
            "id",
            "field_1",
            "field_2",
        ]


class TestEmptySourceColumnsFailRaises(_EmptyTempColumns):
    """on_schema_change='fail' must report the metadata failure, not a bogus column diff."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_fail.sql": _MODEL_FAIL}

    def test_reports_metadata_failure_not_out_of_sync(self, project):
        run_dbt(["run", "--select", "incremental_fail"])

        results = run_dbt(["run", "--select", "incremental_fail"], expect_pass=False)
        assert len(results) == 1
        assert "Could not read any columns" in results[0].message

        # Without the guard this path raises the generic out-of-sync error, which lists every
        # target column as removed and sends the user looking for a schema change that never
        # happened. Asserting its absence is what distinguishes the two failures.
        assert "out of sync" not in results[0].message

        assert _column_names(project, "incremental_fail") == ["id", "field_1", "field_2"]


class TestEmptySourceColumnsAppendNewColumnsWarns(_EmptyTempColumns):
    """append_new_columns cannot drop anything, so it warns and the run still succeeds."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_append_new_columns.sql": _MODEL_APPEND_NEW_COLUMNS}

    def test_warns_and_run_still_succeeds(self, project):
        run_dbt(["run", "--select", "incremental_append_new_columns"])
        assert _column_names(project, "incremental_append_new_columns") == [
            "id",
            "field_1",
            "field_2",
        ]

        # Add a column, so this run has something append_new_columns would normally append.
        write_file(
            _MODEL_APPEND_NEW_COLUMNS_DRIFTED,
            project.project_root,
            "models",
            "incremental_append_new_columns.sql",
        )

        results, logs = run_dbt_and_capture(["run", "--select", "incremental_append_new_columns"])
        assert len(results) == 1
        assert "Could not read any columns" in logs

        # The accepted limitation of warning rather than raising: the new column cannot be
        # detected, so it is not appended. The run succeeds using the target's columns.
        assert _column_names(project, "incremental_append_new_columns") == [
            "id",
            "field_1",
            "field_2",
        ]


class TestEmptySourceColumnsAppendNewColumnsWarnError(_EmptyTempColumns):
    """--warn-error escalates the append_new_columns warning, for users who opt into strictness."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_warn_error.sql": _MODEL_APPEND_NEW_COLUMNS}

    def test_warn_error_escalates_to_failure(self, project):
        run_dbt(["run", "--select", "incremental_warn_error"])

        results = run_dbt(
            ["--warn-error", "run", "--select", "incremental_warn_error"], expect_pass=False
        )
        assert len(results) == 1
        assert "Could not read any columns" in results[0].message


class TestEmptySourceColumnsIgnoreStillWorks(_EmptyTempColumns):
    """on_schema_change='ignore' skips the check entirely, so it must remain unaffected."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_ignore.sql": _MODEL_IGNORE}

    def test_ignore_is_unaffected(self, project):
        run_dbt(["run", "--select", "incremental_ignore"])
        results = run_dbt(["run", "--select", "incremental_ignore"])
        assert len(results) == 1
