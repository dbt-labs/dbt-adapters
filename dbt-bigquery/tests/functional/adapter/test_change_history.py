import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture
import dbt.exceptions

incremental_change_history_enabled_model = """
{{
    config(
        materialized='incremental',
        enable_change_history=True
    )
}}
select 1 as id
"""

incremental_change_history_disabled_model = """
{{
    config(
        materialized='incremental',
        enable_change_history=False
    )
}}
select 1 as id
"""

table_change_history_enabled_model = """
{{
    config(
        materialized='table',
        enable_change_history=True
    )
}}
select 1 as id
"""

view_change_history_enabled_model = """
{{
    config(
        materialized='view',
        enable_change_history=True
    )
}}
select 1 as id
"""


class TestChangeHistorySuccessful:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_change_history_enabled.sql": incremental_change_history_enabled_model,
            "incremental_change_history_disabled.sql": incremental_change_history_disabled_model,
            "table_change_history_enabled.sql": table_change_history_enabled_model,
        }

    def get_is_change_history_enabled(self, project, table_name):
        sql = f"""
        select is_change_history_enabled
        from `{project.database}.{project.test_schema}.INFORMATION_SCHEMA.TABLES`
        where table_name = '{table_name}'
        """
        results = project.run_sql(sql, fetch="one")
        return results[0] if results else None

    def test_change_history_options(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        assert (
            self.get_is_change_history_enabled(project, "incremental_change_history_enabled")
            == "YES"
        )
        assert (
            self.get_is_change_history_enabled(project, "incremental_change_history_disabled")
            == "NO"
        )
        assert self.get_is_change_history_enabled(project, "table_change_history_enabled") == "YES"


class TestChangeHistoryFail:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_with_flag.sql": view_change_history_enabled_model,
        }

    def test_view_with_flag_fails(self, project):
        _, stdout = run_dbt_and_capture(["run", "--select", "view_with_flag"], expect_pass=False)
        assert "`enable_change_history` is not supported for views" in stdout
