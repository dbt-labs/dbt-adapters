import os

import pytest

from dbt.tests.util import (
    assert_message_in_logs,
    run_dbt,
    run_dbt_and_capture,
)

from tests.functional.relation_tests.interactive_table_tests import models
from tests.functional.utils import query_row_count, update_model

# Dynamic interactive tables need a standard refresh warehouse. CI provisions DBT_TESTING;
# local runs override via SNOWFLAKE_TEST_ALT_WAREHOUSE. Models read the same env var.
ALT_WAREHOUSE = os.getenv("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TESTING")


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
        assert_message_in_logs(f"warehouse = {ALT_WAREHOUSE}", logs)
        assert query_row_count(project, "my_interactive_dynamic") == 3


class TestInteractiveTableReplacesExistingTable:
    """A model that already exists as a normal table, then switches to
    materialized='interactive_table', must rebuild cleanly via the replace path."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_relation.sql": models.TABLE_RELATION}

    def test_table_converts_to_interactive(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        update_model(project, "my_relation", models.INTERACTIVE_TABLE_STATIC)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("create interactive table", logs)
        # the swap renames the interactive intermediate into place, which exercises
        # the interactive rename dispatch (alter table ... rename to ...)
        assert_message_in_logs("rename to", logs)
        assert query_row_count(project, "my_relation") == 3


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
    """on_configuration_change='apply' replaces when cluster_by or target_lag change."""

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
            models.INTERACTIVE_TABLE_DYNAMIC_CLUSTER_ALTER,
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
            models.INTERACTIVE_TABLE_DYNAMIC_CLUSTER_ALTER,
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


class TestInteractiveTableStaticRebuildsEveryRun:
    """A STATIC interactive table (cluster_by only) does not auto-refresh and cannot be
    diffed via SHOW, so every run must CREATE OR REPLACE — like a normal table."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_static_rebuild.sql": models.INTERACTIVE_TABLE_STATIC}

    def test_static_rerun_replaces(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("create or replace interactive table", logs)
        assert_message_not_in_logs("No configuration changes were identified on:", logs)


class TestInteractiveTableStaticRebuildsOnSqlChange:
    """A SQL-only change to a static interactive table is reflected on the next run,
    because static tables always CREATE OR REPLACE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_static_sql.sql": models.INTERACTIVE_TABLE_STATIC}

    def test_sql_change_rebuilds(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        assert query_row_count(project, "my_interactive_static_sql") == 3

        update_model(
            project,
            "my_interactive_static_sql",
            models.INTERACTIVE_TABLE_STATIC_SQL_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("create or replace interactive table", logs)
        assert query_row_count(project, "my_interactive_static_sql") == 2


class TestInteractiveTableMultiColumnClusterByNoOp:
    """Re-running a dynamic interactive table with a multi-column cluster_by must not
    CREATE OR REPLACE. Snowflake returns the key wrapped, e.g. '(id, value)'; without
    correct normalization the no-op rerun would falsely detect a cluster_by change."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_multicol_noop.sql": models.INTERACTIVE_TABLE_DYNAMIC_MULTICOL}

    def test_multicol_noop_run_does_not_replace(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_not_in_logs("create or replace interactive table", logs)
        assert_message_in_logs("No configuration changes were identified on:", logs)


def _provision_interactive_warehouses(project, count):
    """Create `count` interactive warehouses named off the (unique) test schema, and
    publish them to the env var the attach models read. Returns the warehouse names."""
    names = [f"dbt_test_iw_{project.test_schema}_{i}".lower() for i in range(count)]
    for name in names:
        project.run_sql(
            f"create or replace interactive warehouse {name} "
            "warehouse_size=xsmall auto_suspend=86400 initially_suspended=true"
        )
    os.environ["SNOWFLAKE_TEST_INTERACTIVE_WHS"] = ",".join(names)
    return names


class TestInteractiveTableAttachesToWarehouse:
    """A model with `snowflake_interactive_warehouses` attaches after the build. A passing
    run proves the ALTER succeeded against the real warehouse."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_attach.sql": models.INTERACTIVE_TABLE_STATIC_ATTACH}

    @pytest.fixture(scope="class", autouse=True)
    def interactive_warehouses(self, project):
        names = _provision_interactive_warehouses(project, 1)
        yield names
        for name in names:
            project.run_sql(f"drop warehouse if exists {name}")

    def test_attaches_after_build(self, project, interactive_warehouses):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs(f"alter warehouse {interactive_warehouses[0]} add tables", logs)


class TestInteractiveTableAttachesToMultipleWarehouses:
    """A list of warehouses attaches to each one."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_attach_multi.sql": models.INTERACTIVE_TABLE_STATIC_ATTACH}

    @pytest.fixture(scope="class", autouse=True)
    def interactive_warehouses(self, project):
        names = _provision_interactive_warehouses(project, 2)
        yield names
        for name in names:
            project.run_sql(f"drop warehouse if exists {name}")

    def test_attaches_to_each(self, project, interactive_warehouses):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])
        for name in interactive_warehouses:
            assert_message_in_logs(f"alter warehouse {name} add tables", logs)


class TestInteractiveTableDynamicReattachesEveryRun:
    """Attach runs on every run, including a dynamic no-op rerun that doesn't rebuild."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_interactive_attach_dyn.sql": models.INTERACTIVE_TABLE_DYNAMIC_ATTACH}

    @pytest.fixture(scope="class", autouse=True)
    def interactive_warehouses(self, project):
        names = _provision_interactive_warehouses(project, 1)
        yield names
        for name in names:
            project.run_sql(f"drop warehouse if exists {name}")

    def test_reattaches_on_noop_run(self, project, interactive_warehouses):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_not_in_logs("create or replace interactive table", logs)
        assert_message_in_logs(f"alter warehouse {interactive_warehouses[0]} add tables", logs)
