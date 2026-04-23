import pytest

from dbt.tests.util import (
    assert_message_in_logs,
    run_dbt,
    run_dbt_and_capture,
)

from tests.functional.relation_tests.interactive_table_tests import models
from tests.functional.utils import query_row_count, update_model


def assert_message_not_in_logs(message: str, logs: str) -> None:
    assert_message_in_logs(message, logs, expected_pass=False)


class TestInteractiveTableStaticBasic:
    """Create a static interactive table (no target_lag / warehouse)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_static.sql": models.INTERACTIVE_TABLE_STATIC}

    def test_create_static_interactive_table(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("create interactive table", logs)
        assert_message_in_logs("cluster by (id)", logs)
        assert_message_not_in_logs("target_lag", logs)
        assert query_row_count(project, "my_interactive_static") == 3


class TestInteractiveTableDynamicBasic:
    """Create a dynamic interactive table (target_lag + warehouse)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_dynamic.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    def test_create_dynamic_interactive_table(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("create interactive table", logs)
        assert_message_in_logs("cluster by (id)", logs)
        assert_message_in_logs("target_lag = '2 minutes'", logs)
        assert_message_in_logs("warehouse = DBT_TESTING", logs)
        assert query_row_count(project, "my_interactive_dynamic") == 3


class TestInteractiveTableMissingClusterByRaises:
    """`cluster_by` is required — compilation must fail without it."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_missing_cluster.sql": models.INTERACTIVE_TABLE_MISSING_CLUSTER_BY}

    def test_missing_cluster_by_raises(self, project):
        run_dbt(["seed"])
        result = run_dbt(["run"], expect_pass=False)
        assert any(
            "cluster_by" in (r.message or "").lower() for r in result.results
        ), f"expected cluster_by compilation error, got: {[r.message for r in result.results]}"


class TestInteractiveTableTargetLagWithoutWarehouseRaises:
    """target_lag requires snowflake_warehouse — compilation must fail otherwise."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_lag_no_wh.sql": models.INTERACTIVE_TABLE_LAG_NO_WAREHOUSE}

    def test_target_lag_without_warehouse_raises(self, project):
        run_dbt(["seed"])
        result = run_dbt(["run"], expect_pass=False)
        assert any(
            "snowflake_warehouse" in (r.message or "").lower() for r in result.results
        ), f"expected snowflake_warehouse compilation error, got: {[r.message for r in result.results]}"


class TestInteractiveTableFullRefreshReplaces:
    """--full-refresh issues CREATE OR REPLACE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_full_refresh.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    def test_full_refresh_replaces(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])
        assert_message_in_logs("create or replace interactive table", logs)


class TestInteractiveTableConfigChangeApply:
    """on_configuration_change='apply' replaces when cluster_by / target_lag / warehouse change."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_cfg_change.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, project, setup_class):
        run_dbt(["run", "--full-refresh"])
        yield
        update_model(project, "interactive_cfg_change", models.INTERACTIVE_TABLE_DYNAMIC)

    def test_cluster_by_change_triggers_replace(self, project):
        update_model(
            project,
            "interactive_cfg_change",
            models.INTERACTIVE_TABLE_STATIC_CLUSTER_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("create or replace interactive table", logs)
        assert_message_in_logs("cluster by (value)", logs)

    def test_target_lag_change_triggers_replace(self, project):
        update_model(
            project,
            "interactive_cfg_change",
            models.INTERACTIVE_TABLE_DYNAMIC_LAG_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("create or replace interactive table", logs)
        assert_message_in_logs("target_lag = '5 minutes'", logs)


class TestInteractiveTableConfigChangeContinueNoOp:
    """on_configuration_change='continue' skips the rebuild and warns."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_cfg_continue.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "continue"}}

    def test_continue_skips_rebuild(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        update_model(
            project,
            "interactive_cfg_continue",
            models.INTERACTIVE_TABLE_STATIC_CLUSTER_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_not_in_logs("create or replace interactive table", logs)
        assert_message_in_logs("on_configuration_change` was set to `continue`", logs)


class TestInteractiveTableConfigChangeFail:
    """on_configuration_change='fail' aborts the run when a change is detected."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_cfg_fail.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "fail"}}

    def test_fail_raises_on_config_change(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        update_model(project, "interactive_cfg_fail", models.INTERACTIVE_TABLE_DYNAMIC_LAG_ALTER)
        result = run_dbt(["run"], expect_pass=False)
        assert any(
            "on_configuration_change" in (r.message or "").lower() for r in result.results
        ), f"expected fail-fast error, got: {[r.message for r in result.results]}"


class TestInteractiveTableNoChangeIsNoOp:
    """Re-running with no config changes should not CREATE OR REPLACE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_noop.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    def test_noop_run_does_not_replace(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_not_in_logs("create or replace interactive table", logs)
        assert_message_in_logs("No configuration changes were identified on:", logs)
