import os

import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.interactive_table_tests import models
from tests.functional.utils import (
    describe_interactive_table,
    query_relation_type,
    update_model,
)


# Get the alternate warehouse from environment, default to DBT_TESTING if not set.
ALT_WAREHOUSE = os.getenv("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TESTING")

# An interactive-type warehouse (created via `CREATE WAREHOUSE ... WAREHOUSE_TYPE =
# INTERACTIVE`), needed only for the attach/detach tests. Task 11 (live verification)
# must set this env var to a real interactive warehouse, or update the default here.
INTERACTIVE_WAREHOUSE = os.getenv(
    "SNOWFLAKE_TEST_INTERACTIVE_WAREHOUSE", "DBT_TESTING_INTERACTIVE"
)


def assert_message_not_in_logs(message: str, logs: str):
    assert_message_in_logs(message, logs, expected_pass=False)


class TestBasic:
    """Smoke test: both a dynamic and a static interactive table can be created."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_interactive_table.sql": models.INTERACTIVE_TABLE_DYNAMIC,
            "my_static_interactive_table.sql": models.INTERACTIVE_TABLE_STATIC,
        }

    def test_create_interactive_table(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        assert query_relation_type(project, "my_interactive_table") == "interactive_table"
        assert query_relation_type(project, "my_static_interactive_table") == "interactive_table"


class TestCompileValidation:
    """Task 2's four compile-time validations must raise a CompilationError at
    `dbt run`, before any SQL reaches Snowflake.

    Each model is run individually via `--select` so one validation failure
    doesn't prevent the others in this class from being exercised. Following
    the precedent at `tests/functional/relation_tests/test_relation_type_change.py`
    and `tests/functional/warehouse_test/test_warehouses.py::TestInvalidConfigWarehouse`
    (both in this repo): `run_dbt([...], expect_pass=False)` returns the RunResults
    list, and the failed node's `.message` carries the raised exception's text.
    """

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "it_missing_cluster_by.sql": models.INTERACTIVE_TABLE_MISSING_CLUSTER_BY,
            "it_blank_cluster_by.sql": models.INTERACTIVE_TABLE_BLANK_CLUSTER_BY,
            "it_iceberg_format.sql": models.INTERACTIVE_TABLE_ICEBERG_FORMAT,
            "it_transient_true.sql": models.INTERACTIVE_TABLE_TRANSIENT_TRUE,
            "it_target_lag_no_warehouse.sql": models.INTERACTIVE_TABLE_TARGET_LAG_NO_WAREHOUSE,
        }

    def test_missing_cluster_by_raises_compilation_error(self, project):
        results = run_dbt(["run", "--select", "it_missing_cluster_by"], expect_pass=False)
        assert "require a non-empty `cluster_by` config" in results[0].message

    def test_blank_cluster_by_raises_compilation_error(self, project):
        results = run_dbt(["run", "--select", "it_blank_cluster_by"], expect_pass=False)
        assert "require a non-empty `cluster_by` config" in results[0].message

    def test_iceberg_table_format_raises_compilation_error(self, project):
        results = run_dbt(["run", "--select", "it_iceberg_format"], expect_pass=False)
        assert "do not support `table_format: iceberg`" in results[0].message

    def test_transient_true_raises_compilation_error(self, project):
        results = run_dbt(["run", "--select", "it_transient_true"], expect_pass=False)
        assert "do not support `transient: true`" in results[0].message

    def test_target_lag_without_warehouse_raises_compilation_error(self, project):
        results = run_dbt(["run", "--select", "it_target_lag_no_warehouse"], expect_pass=False)
        assert "require a warehouse" in results[0].message


class TestTargetLagValueChange:
    """A target_lag value-to-value change is alterable in place."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_table_target_lag.sql": models.INTERACTIVE_TABLE_DYNAMIC}

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
        update_model(project, "interactive_table_target_lag", models.INTERACTIVE_TABLE_DYNAMIC)

    def test_alter_target_lag_value(self, project):
        update_model(
            project,
            "interactive_table_target_lag",
            models.INTERACTIVE_TABLE_DYNAMIC_TARGET_LAG_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_in_logs("alter interactive table", logs)
        assert_message_in_logs("target_lag = '2 hours'", logs)
        assert_message_not_in_logs("create or replace interactive table", logs)

        dt = describe_interactive_table(project, "interactive_table_target_lag")
        assert dt.target_lag == "2 hours"


class TestRefreshWarehouseChange:
    """A refresh_warehouse-only change is alterable in place, not a replace."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_table_refresh_wh.sql": models.INTERACTIVE_TABLE_DYNAMIC}

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
        update_model(project, "interactive_table_refresh_wh", models.INTERACTIVE_TABLE_DYNAMIC)

    def test_alter_refresh_warehouse(self, project):
        update_model(
            project,
            "interactive_table_refresh_wh",
            models.INTERACTIVE_TABLE_DYNAMIC_REFRESH_WAREHOUSE_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_in_logs("alter interactive table", logs)
        assert_message_not_in_logs("create or replace interactive table", logs)

        dt = describe_interactive_table(project, "interactive_table_refresh_wh")
        assert dt.refresh_warehouse is not None
        assert ALT_WAREHOUSE.upper() in dt.refresh_warehouse.upper()


class TestInitializationWarehouseChanges:
    """snowflake_initialization_warehouse changes, mirroring
    dynamic_table_tests.test_configuration_changes.TestInitializationWarehouseChanges.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "interactive_table_init_wh.sql": models.INTERACTIVE_TABLE_DYNAMIC_WITH_INIT_WAREHOUSE,
        }

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
        update_model(
            project,
            "interactive_table_init_wh",
            models.INTERACTIVE_TABLE_DYNAMIC_WITH_INIT_WAREHOUSE,
        )

    def test_create_with_initialization_warehouse(self, project):
        it = describe_interactive_table(project, "interactive_table_init_wh")
        assert it.snowflake_initialization_warehouse is not None
        assert ALT_WAREHOUSE.upper() in it.snowflake_initialization_warehouse.upper()

    def test_alter_initialization_warehouse(self, project):
        it_before = describe_interactive_table(project, "interactive_table_init_wh")
        assert it_before.snowflake_initialization_warehouse is not None

        update_model(
            project,
            "interactive_table_init_wh",
            models.INTERACTIVE_TABLE_DYNAMIC_WITH_INIT_WAREHOUSE_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_not_in_logs("create or replace interactive table", logs)

        it_after = describe_interactive_table(project, "interactive_table_init_wh")
        assert it_after.snowflake_initialization_warehouse is not None
        assert "DBT_TESTING" in it_after.snowflake_initialization_warehouse.upper()

    def test_unset_initialization_warehouse(self, project):
        it_before = describe_interactive_table(project, "interactive_table_init_wh")
        assert it_before.snowflake_initialization_warehouse is not None

        update_model(
            project,
            "interactive_table_init_wh",
            models.INTERACTIVE_TABLE_DYNAMIC_WITHOUT_INIT_WAREHOUSE,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_in_logs("unset initialization_warehouse", logs)
        assert_message_not_in_logs("create or replace interactive table", logs)

        it_after = describe_interactive_table(project, "interactive_table_init_wh")
        assert it_after.snowflake_initialization_warehouse is None


class TestClusterByChange:
    """cluster_by has no ALTER path on an interactive table (001003), so a
    change must force a full CREATE OR REPLACE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_table_cluster.sql": models.INTERACTIVE_TABLE_DYNAMIC}

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
        update_model(project, "interactive_table_cluster", models.INTERACTIVE_TABLE_DYNAMIC)

    def test_cluster_by_change_forces_replace(self, project):
        update_model(
            project,
            "interactive_table_cluster",
            models.INTERACTIVE_TABLE_DYNAMIC_CLUSTER_BY_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("create or replace interactive table", logs)
        assert_message_not_in_logs("alter interactive table", logs)

        it = describe_interactive_table(project, "interactive_table_cluster")
        assert it.cluster_by is not None
        assert "VALUE" in it.cluster_by.upper()


class TestTargetLagTransitions:
    """Snowflake rejects ALTER for both a dynamic->static and a static->dynamic
    target_lag transition (001422 / 001420, confirmed live 2026-08-25), so both
    directions must force a full CREATE OR REPLACE.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "interactive_table_d2s.sql": models.INTERACTIVE_TABLE_DYNAMIC,
            "interactive_table_s2d.sql": models.INTERACTIVE_TABLE_STATIC,
        }

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
        update_model(project, "interactive_table_d2s", models.INTERACTIVE_TABLE_DYNAMIC)
        update_model(project, "interactive_table_s2d", models.INTERACTIVE_TABLE_STATIC)

    def test_dynamic_to_static_forces_replace(self, project):
        update_model(project, "interactive_table_d2s", models.INTERACTIVE_TABLE_STATIC)
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "interactive_table_d2s"])

        assert_message_in_logs("create or replace interactive table", logs)
        assert_message_not_in_logs("alter interactive table", logs)

    def test_static_to_dynamic_forces_replace(self, project):
        update_model(project, "interactive_table_s2d", models.INTERACTIVE_TABLE_DYNAMIC)
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "interactive_table_s2d"])

        assert_message_in_logs("create or replace interactive table", logs)
        assert_message_not_in_logs("alter interactive table", logs)


class TestStaticTableNoDiffRegression:
    """Task 1 regression: a project-wide snowflake_initialization_warehouse config
    must not force a phantom diff on a static (non-dynamic) interactive table --
    the `is_dynamic` gate in `interactive_table_config_changeset` must suppress it.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_table_static_regression.sql": models.INTERACTIVE_TABLE_STATIC}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "snowflake_initialization_warehouse": ALT_WAREHOUSE,
                "on_configuration_change": "apply",
            }
        }

    def test_static_table_with_project_wide_init_warehouse_produces_no_diff(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        model_qualified_name = (
            f"{project.database}.{project.test_schema}.interactive_table_static_regression"
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs(
            f"No configuration changes were identified on: `{model_qualified_name}`. Continuing.",
            logs,
        )
        assert_message_not_in_logs("create or replace interactive table", logs)
        assert_message_not_in_logs("alter interactive table", logs)


class TestWarehouseAttachDetach:
    """Task 6: snowflake_interactive_warehouses attach/detach via
    `alter warehouse ... add/drop tables`, driven by the warehouse-sync macro
    that runs on every materialization pass regardless of whether the table
    itself changed.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_table_wh_sync.sql": models.INTERACTIVE_TABLE_DYNAMIC}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_adding_warehouse_attaches(self, project, setup_class):
        update_model(
            project,
            "interactive_table_wh_sync",
            models.INTERACTIVE_TABLE_WITH_INTERACTIVE_WAREHOUSES,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs(f"alter warehouse {INTERACTIVE_WAREHOUSE} add tables", logs)

    def test_removing_warehouse_detaches(self, project, setup_class):
        update_model(
            project,
            "interactive_table_wh_sync",
            models.INTERACTIVE_TABLE_WITH_INTERACTIVE_WAREHOUSES,
        )
        run_dbt(["run"])

        update_model(
            project,
            "interactive_table_wh_sync",
            models.INTERACTIVE_TABLE_WITHOUT_INTERACTIVE_WAREHOUSES,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs(f"alter warehouse {INTERACTIVE_WAREHOUSE} drop tables", logs)


class TestStaticNoOpIdempotency:
    """Task 5: a static interactive table with nothing changed must no-op on a
    second run rather than unconditionally rebuilding it. No
    snowflake_interactive_warehouses config means no warehouse-sync statements
    fire either, so no statement at all is expected on the second run.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"interactive_table_static_idempotent.sql": models.INTERACTIVE_TABLE_STATIC}

    def test_static_table_no_op_on_second_run(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_not_in_logs("create or replace interactive table", logs)
        assert_message_not_in_logs("alter interactive table", logs)
        assert_message_not_in_logs("alter warehouse", logs)


class Changes:
    """Shared on_configuration_change apply/continue/fail scaffolding, mirroring
    dynamic_table_tests.test_configuration_changes.Changes. `interactive_table_alter`
    exercises an alterable change (target_lag); `interactive_table_replace` exercises
    a full-refresh-only change (cluster_by).
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "interactive_table_alter.sql": models.INTERACTIVE_TABLE_DYNAMIC,
            "interactive_table_replace.sql": models.INTERACTIVE_TABLE_DYNAMIC,
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, project, setup_class):
        # make sure the model in the data reflects the files each time
        run_dbt(["run", "--full-refresh"])
        self.assert_changes_are_not_applied(project)

        update_model(
            project, "interactive_table_alter", models.INTERACTIVE_TABLE_DYNAMIC_TARGET_LAG_ALTER
        )
        update_model(
            project,
            "interactive_table_replace",
            models.INTERACTIVE_TABLE_DYNAMIC_CLUSTER_BY_ALTER,
        )

        yield

        update_model(project, "interactive_table_alter", models.INTERACTIVE_TABLE_DYNAMIC)
        update_model(project, "interactive_table_replace", models.INTERACTIVE_TABLE_DYNAMIC)

    def assert_changes_are_applied(self, project):
        altered = describe_interactive_table(project, "interactive_table_alter")
        assert altered.target_lag == "2 hours"  # this updated

        replaced = describe_interactive_table(project, "interactive_table_replace")
        assert replaced.cluster_by is not None
        assert "VALUE" in replaced.cluster_by.upper()  # this updated

    def assert_changes_are_not_applied(self, project):
        altered = describe_interactive_table(project, "interactive_table_alter")
        assert altered.target_lag == "1 hour"  # this would have updated, but didn't

        replaced = describe_interactive_table(project, "interactive_table_replace")
        assert replaced.cluster_by is not None
        assert "ID" in replaced.cluster_by.upper()  # this would have updated, but didn't

    def test_full_refresh_is_always_successful(self, project):
        # this always passes and always changes the configuration, regardless of
        # on_configuration_change and regardless of whether the changes require a
        # replace versus an alter
        run_dbt(["run", "--full-refresh"])
        self.assert_changes_are_applied(project)


class TestChangesApply(Changes):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_changes_are_applied(self, project):
        # this passes and changes the configuration
        run_dbt(["run"])
        self.assert_changes_are_applied(project)


class TestChangesContinue(Changes):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "continue"}}

    def test_changes_are_not_applied(self, project):
        # this passes but does not change the configuration
        run_dbt(["run"])
        self.assert_changes_are_not_applied(project)


class TestChangesFail(Changes):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "fail"}}

    def test_changes_are_not_applied(self, project):
        # this fails and does not change the configuration
        run_dbt(["run"], expect_pass=False)
        self.assert_changes_are_not_applied(project)
