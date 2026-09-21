from typing import List

import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.data_metric_function_tests import models
from tests.functional.utils import update_model


def assert_message_not_in_logs(message: str, logs: str):
    assert_message_in_logs(message, logs, expected_pass=False)


def query_attached_dmfs(project, relation_name: str) -> List[str]:
    """Return the metric names currently attached to a relation, read back from the
    real-time INFORMATION_SCHEMA table function that the adapter converges against."""
    sql = f"""
    select metric_name
    from table(
        {project.database}.information_schema.data_metric_function_references(
            ref_entity_name => '{project.database}.{project.test_schema}.{relation_name}',
            ref_entity_domain => 'table'
        )
    )
    order by metric_name
    """
    return [row[0].upper() for row in project.run_sql(sql, fetch="all")]


class TestDataMetricFunctionsBasic:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_dmf_table.sql": models.TABLE_TWO_DMFS}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])

    def test_dmfs_and_schedule_applied_on_build(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("set data_metric_schedule = '5 MINUTE'", logs)
        assert_message_in_logs("add data metric function snowflake.core.null_count on (id)", logs)
        assert_message_in_logs(
            "add data metric function snowflake.core.duplicate_count on (id)", logs
        )

        assert query_attached_dmfs(project, "my_dmf_table") == ["DUPLICATE_COUNT", "NULL_COUNT"]

    def test_second_run_is_idempotent(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("data metric functions already in sync", logs)
        assert_message_not_in_logs("add data metric function", logs)

        assert query_attached_dmfs(project, "my_dmf_table") == ["DUPLICATE_COUNT", "NULL_COUNT"]


class TestDataMetricFunctionsConverge:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_dmf_table.sql": models.TABLE_TWO_DMFS}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        update_model(project, "my_dmf_table", models.TABLE_TWO_DMFS)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_removed_dmf_is_dropped(self, project):
        assert query_attached_dmfs(project, "my_dmf_table") == ["DUPLICATE_COUNT", "NULL_COUNT"]

        update_model(project, "my_dmf_table", models.TABLE_ONE_DMF)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs(
            "drop data metric function snowflake.core.duplicate_count on (id)", logs
        )
        assert query_attached_dmfs(project, "my_dmf_table") == ["NULL_COUNT"]


class TestDataMetricFunctionsIncremental:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_dmf_incremental.sql": models.INCREMENTAL_ONE_DMF}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])

    def test_dmf_applied_from_incremental(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("add data metric function snowflake.core.null_count on (id)", logs)
        assert query_attached_dmfs(project, "my_dmf_incremental") == ["NULL_COUNT"]

        # A second, incremental (non-full-refresh) run must not re-add the DMF.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_not_in_logs("add data metric function", logs)
        assert query_attached_dmfs(project, "my_dmf_incremental") == ["NULL_COUNT"]
