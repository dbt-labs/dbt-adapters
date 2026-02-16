"""Functional tests for query_group (profile and per-model config)."""

import pytest
from dbt.tests.util import run_dbt

# Minimal snapshot (single column, check on id) so query_group can be asserted via post-hook.
# Kept simple to avoid Redshift type-inference issues that can occur with more complex snapshot queries.
snapshots__snapshot_query_group_sql = """
{% snapshot snapshot_query_group %}
    {{ config(unique_key='id', strategy='check', check_cols=['id']) }}
    select 1 as id
{% endsnapshot %}
"""

models__table_model_query_group_sql = """
{{ config(materialized = 'table') }}
select 1 as id
"""


models__models_config_yml = """
version: 2

models:
  - name: view_model_query_group
    columns:
      - name: id
        data_tests:
          - unique
"""


models__view_model_query_group_sql = """
{{ config(materialized = 'view') }}
select 1 as id
"""


models__incremental_model_query_group_sql = """
{{ config(materialized = 'incremental', unique_key = 'id') }}
select 1 as id
"""


macros__check_query_group_sql = """
{% macro check_query_group() %}
  {% if execute %}
    {% set current = get_current_query_group() %}
    {% set expected = var("query_group") %}
    {% if current != expected %}
      {{ exceptions.raise_compiler_error("Query group not set: expected '" ~ expected ~ "', got '" ~ (current or '') ~ "'") }}
    {% endif %}
  {% endif %}
{% endmacro %}
"""


seeds__seed_query_group_csv = """id
1
""".strip()


class TestQueryGroupPerModel:
    """Per-model query_group via project config and post-hook check."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_query_group.sql": models__table_model_query_group_sql,
            "view_model_query_group.sql": models__view_model_query_group_sql,
            "incremental_model_query_group.sql": models__incremental_model_query_group_sql,
            "models_config.yml": models__models_config_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_query_group.sql": snapshots__snapshot_query_group_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_query_group.sql": macros__check_query_group_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_query_group.csv": seeds__seed_query_group_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self, prefix):
        return {
            "config-version": 2,
            "models": {"query_group": prefix, "post-hook": "{{ check_query_group() }}"},
            "seeds": {"query_group": prefix, "post-hook": "{{ check_query_group() }}"},
            "snapshots": {"query_group": prefix, "post-hook": "{{ check_query_group() }}"},
            "tests": {"test": {"query_group": prefix, "post-hook": "{{ check_query_group() }}"}},
        }

    def build_all_with_query_group(self, project, prefix):
        run_dbt(["build", "--vars", '{{"query_group": "{}"}}'.format(prefix)])

    def test_query_group_per_model(self, project, prefix):
        self.build_all_with_query_group(project, prefix)
        self.build_all_with_query_group(project, prefix)


class TestQueryGroupProfile:
    """Profile-level query_group (applies to all queries on the connection)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_query_group.sql": models__table_model_query_group_sql,
            "view_model_query_group.sql": models__view_model_query_group_sql,
            "incremental_model_query_group.sql": models__incremental_model_query_group_sql,
            "models_config.yml": models__models_config_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_query_group.sql": snapshots__snapshot_query_group_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_query_group.sql": macros__check_query_group_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_query_group.csv": seeds__seed_query_group_csv}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, prefix):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["query_group"] = prefix
        return outputs

    @pytest.fixture(scope="class")
    def project_config_update(self, prefix):
        return {
            "config-version": 2,
            "models": {"post-hook": "{{ check_query_group() }}"},
            "seeds": {"post-hook": "{{ check_query_group() }}"},
            "snapshots": {"post-hook": "{{ check_query_group() }}"},
            "tests": {"test": {"post-hook": "{{ check_query_group() }}"}},
        }

    def test_query_group_from_profile(self, project, prefix):
        # Profile sets query_group on connection; post-hook asserts session has it
        run_dbt(["build", "--vars", '{{"query_group": "{}"}}'.format(prefix)])
        run_dbt(["build", "--vars", '{{"query_group": "{}"}}'.format(prefix)])
