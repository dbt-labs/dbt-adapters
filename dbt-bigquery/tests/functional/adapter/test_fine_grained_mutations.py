from typing import Any, Sequence, cast

import pytest  # type: ignore
from dbt.tests.util import run_dbt, run_dbt_and_capture


incremental_fgm_enabled_model = """
{{
    config(
        materialized='incremental',
        enable_fine_grained_mutations=True
    )
}}
select 1 as id
"""


incremental_fgm_disabled_model = """
{{
    config(
        materialized='incremental',
        enable_fine_grained_mutations=False
    )
}}
select 1 as id
"""


table_fgm_enabled_model = """
{{
    config(
        materialized='table',
        enable_fine_grained_mutations=True
    )
}}
select 1 as id
"""


view_fgm_enabled_model = """
{{
    config(
        materialized='view',
        enable_fine_grained_mutations=True
    )
}}
select 1 as id
"""


class TestFineGrainedMutationsSuccessful:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_fgm_enabled.sql": incremental_fgm_enabled_model,
            "incremental_fgm_disabled.sql": incremental_fgm_disabled_model,
            "table_fgm_enabled.sql": table_fgm_enabled_model,
        }

    def get_is_fgm_enabled(self, project, table_name):
        sql = f"""
        select is_fine_grained_mutations_enabled
        from `{project.database}.{project.test_schema}.INFORMATION_SCHEMA.TABLES`
        where table_name = '{table_name}'
        """
        results = project.run_sql(sql, fetch="one")
        return results[0] if results else None

    def test_fgm_options(self, project):
        results = cast(Sequence[Any], run_dbt(["run"]))
        assert len(results) == 3

        assert self.get_is_fgm_enabled(project, "incremental_fgm_enabled") == "YES"
        assert self.get_is_fgm_enabled(project, "incremental_fgm_disabled") == "NO"
        assert self.get_is_fgm_enabled(project, "table_fgm_enabled") == "YES"


class TestFineGrainedMutationsFail:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_with_flag.sql": view_fgm_enabled_model,
        }

    def test_view_with_flag_fails(self):
        _, stdout = run_dbt_and_capture(["run", "--select", "view_with_flag"], expect_pass=False)
        assert "`enable_fine_grained_mutations` is not supported for views on BigQuery." in stdout
