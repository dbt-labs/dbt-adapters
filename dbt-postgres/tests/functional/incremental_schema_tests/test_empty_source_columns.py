"""
Regression tests for the empty-source-columns guard in default__check_for_schema_changes.

When column introspection of the incremental temp relation returns no rows, dbt used to treat
that as "the source has no columns" and, under on_schema_change='sync_all_columns', drop every
column from the target table. This is reachable in the wild: on Redshift, connecting to a
datashare consumer database makes temporary relations invisible to information_schema.columns,
pg_attribute and svv_columns, while the relation itself remains fully queryable
(dbt-labs/dbt-adapters#1947, #1991).

These tests simulate that condition adapter-agnostically by overriding
postgres__get_columns_in_relation to return an empty list for the temp relation only.
"""

from dbt.tests.util import run_dbt
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


_MODEL_IGNORE = _MODEL_SYNC_ALL_COLUMNS.replace(
    "on_schema_change='sync_all_columns'", "on_schema_change='ignore'"
)


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


class TestEmptySourceColumnsRaises:
    """sync_all_columns must fail loudly rather than drop every target column."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_sync_all_columns.sql": _MODEL_SYNC_ALL_COLUMNS}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"empty_temp_columns.sql": _MACRO_EMPTY_TEMP_COLUMNS}

    def test_raises_and_preserves_target_columns(self, project):
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
        results = run_dbt(["run", "--select", "incremental_sync_all_columns"], expect_pass=False)
        assert len(results) == 1
        assert "Could not read any columns" in results[0].message
        assert "metadata failure" in results[0].message

        # The target table must be untouched -- this is the actual regression.
        assert _column_names(project, "incremental_sync_all_columns") == [
            "id",
            "field_1",
            "field_2",
        ]


class TestEmptySourceColumnsIgnoreStillWorks:
    """on_schema_change='ignore' skips the check entirely, so it must remain unaffected."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_ignore.sql": _MODEL_IGNORE}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"empty_temp_columns.sql": _MACRO_EMPTY_TEMP_COLUMNS}

    def test_ignore_is_unaffected(self, project):
        run_dbt(["run", "--select", "incremental_ignore"])
        results = run_dbt(["run", "--select", "incremental_ignore"])
        assert len(results) == 1
