"""Functional tests for query_group (profile and per-model config)."""

import pytest
from dbt.tests.util import run_dbt

models__table_query_group_sql = """
{{ config(materialized = 'table', query_group = 'model_query_group') }}
select 1 as id
"""

models__table_no_query_group_sql = """
{{ config(materialized = 'table') }}
select 1 as id
"""

macros__check_query_group_sql = """
{% macro check_query_group(expected_query_group) %}
  {% if execute %}
    {% set result = run_query("SELECT current_setting('query_group') AS value").rows[0]['value'] %}
    {% if result != expected_query_group %}
      {{ exceptions.raise_compiler_error("Query group not set: expected '" ~ expected_query_group ~ "', got '" ~ (result or '') ~ "'") }}
    {% endif %}
  {% endif %}
{% endmacro %}
"""


class TestQueryGroupPerModel:
    """Per-model query_group via project config and post-hook check."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_query_group.sql": models__table_query_group_sql,
            "table_no_query_group.sql": models__table_no_query_group_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_query_group.sql": macros__check_query_group_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "models": {
                "test": {
                    "table_query_group": {
                        "post-hook": "{{ check_query_group('model_query_group') }}"
                    },
                    "table_no_query_group": {"post-hook": "{{ check_query_group('default') }}"},
                },
            },
        }

    def test_query_group_per_model(self, project):
        run_dbt(["run"])


class TestQueryGroupProfile:
    """Profile-level query_group (applies to all queries on the connection)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_query_group.sql": models__table_query_group_sql,
            "table_no_query_group.sql": models__table_no_query_group_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_query_group.sql": macros__check_query_group_sql}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["query_group"] = "profile_query_group"
        return outputs

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "models": {
                "test": {
                    "table_query_group": {
                        "post-hook": "{{ check_query_group('model_query_group') }}"
                    },
                    "table_no_query_group": {
                        "post-hook": "{{ check_query_group('profile_query_group') }}"
                    },
                },
            },
        }

    def test_query_group_from_profile(self, project):
        # Profile sets query_group on connection; post-hook asserts session has it
        run_dbt(["run"])
