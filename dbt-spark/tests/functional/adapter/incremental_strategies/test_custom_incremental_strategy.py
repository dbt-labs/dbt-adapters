import pytest

from dbt.tests.util import run_dbt

custom_strategy_macro_sql = """
{% macro get_incremental_my_custom_sql(arg_dict) %}
  insert into {{ arg_dict["target_relation"] }}
  select {{ arg_dict["dest_columns"] | map(attribute="name") | join(", ") }}
  from {{ arg_dict["temp_relation"] }}
{% endmacro %}
"""

custom_strategy_model_sql = """
{{ config(materialized="incremental", incremental_strategy="my_custom") }}
select cast(1 as bigint) as id
"""

missing_strategy_model_sql = """
{{ config(materialized="incremental", incremental_strategy="does_not_exist") }}
select cast(1 as bigint) as id
"""


class TestSparkCustomIncrementalStrategy:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"custom_strategy.sql": custom_strategy_macro_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"custom_model.sql": custom_strategy_model_sql}

    def test_custom_strategy_appends(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        results = project.run_sql(
            f"select count(*) from {project.test_schema}.custom_model",
            fetch="one",
        )
        assert results[0] == 2


class TestSparkMissingCustomIncrementalStrategy:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_model.sql": missing_strategy_model_sql}

    def test_missing_strategy_errors(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert (
            'dbt could not find an incremental strategy macro with the name "get_incremental_does_not_exist_sql"'
            in results.results[0].message
        )
