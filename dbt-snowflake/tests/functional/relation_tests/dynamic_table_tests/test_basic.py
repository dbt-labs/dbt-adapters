import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import (
    insert_record,
    query_relation_type,
    query_row_count,
    update_model,
)


def assert_message_not_in_logs(message: str, logs: str):
    assert_message_in_logs(message, logs, expected_pass=False)


class TestBasic:

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dynamic_table.sql": models.DYNAMIC_TABLE,
            "my_dynamic_table_downstream.sql": models.DYNAMIC_TABLE_DOWNSTREAM,
            "my_dynamic_iceberg_table.sql": models.DYNAMIC_ICEBERG_TABLE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def test_dynamic_table_full_refresh(self, project):
        run_dbt(["run", "--full-refresh"])
        assert query_relation_type(project, "my_dynamic_table") == "dynamic_table"
        assert query_relation_type(project, "my_dynamic_table_downstream") == "dynamic_table"
        assert query_relation_type(project, "my_dynamic_iceberg_table") == "dynamic_table"


class TestAutoConfigDoesntFullRefresh:
    """
    AUTO refresh_strategy will be compared accurately with both INCREMENTAL and FULL.
    https://github.com/dbt-labs/dbt-snowflake/issues/1267
    """

    DT_NAME = "my_dynamic_table"

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"explicit_{self.DT_NAME}.sql": models.EXPLICIT_AUTO_DYNAMIC_TABLE,
            f"implicit_{self.DT_NAME}.sql": models.IMPLICIT_AUTO_DYNAMIC_TABLE,
        }

    @pytest.mark.parametrize("test_dt", [f"explicit_{DT_NAME}", f"implicit_{DT_NAME}"])
    def test_auto_config_doesnt_full_refresh(self, project, test_dt):
        model_qualified_name = f"{project.database}.{project.test_schema}.{test_dt}"

        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", f"{test_dt}.sql"])
        assert_message_in_logs(f"create dynamic table {model_qualified_name}", logs)
        assert_message_in_logs("refresh_mode = AUTO", logs)

        _, logs = run_dbt_and_capture(["--debug", "run", "--select", f"{test_dt}.sql"])

        assert_message_not_in_logs(f"create dynamic table {model_qualified_name}", logs)
        assert_message_not_in_logs(f"create or replace dynamic table {model_qualified_name}", logs)
        assert_message_not_in_logs("refresh_mode = AUTO", logs)
        assert_message_in_logs(
            f"No configuration changes were identified on: `{model_qualified_name}`. Continuing.",
            logs,
        )


class TestSchedulerDisabled:
    """Verify SCHEDULER=DISABLE appears in DDL and REFRESH is called."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_scheduler_disabled.sql": models.DYNAMIC_TABLE_SCHEDULER_DISABLED,
        }

    def test_scheduler_disabled_in_create_ddl(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert_message_in_logs("alter dynamic table", logs)
        assert_message_in_logs("Applying REFRESH to:", logs)
        assert query_relation_type(project, "my_dt_scheduler_disabled") == "dynamic_table"


class TestSchedulerEnabled:
    """Verify SCHEDULER=ENABLE with target_lag works."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_scheduler_enabled.sql": models.DYNAMIC_TABLE_SCHEDULER_ENABLED,
        }

    def test_scheduler_enabled_in_create_ddl(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("scheduler = 'ENABLE'", logs)
        assert_message_in_logs("target_lag = '2 minutes'", logs)
        assert_message_not_in_logs("Applying REFRESH to:", logs)
        assert query_relation_type(project, "my_dt_scheduler_enabled") == "dynamic_table"


class TestNoTargetLagDefaultsSchedulerDisabled:
    """Verify missing target_lag defaults scheduler to DISABLE and REFRESH is called."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_no_target_lag.sql": models.DYNAMIC_TABLE_NO_TARGET_LAG,
        }

    def test_no_target_lag_defaults_scheduler_disabled(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert_message_in_logs("alter dynamic table", logs)
        assert_message_in_logs("Applying REFRESH to:", logs)
        assert query_relation_type(project, "my_dt_no_target_lag") == "dynamic_table"


class TestSchedulerInReplaceDDL:
    """Verify scheduler is preserved in CREATE OR REPLACE."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_scheduler_replace.sql": models.DYNAMIC_TABLE_SCHEDULER_DISABLED,
        }

    def test_scheduler_in_replace_ddl(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])

        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert query_relation_type(project, "my_dt_scheduler_replace") == "dynamic_table"


class TestSchedulerConfigChange:
    """Verify scheduler can be changed from DISABLED to ENABLED via ALTER."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_scheduler.sql": models.DYNAMIC_TABLE_SCHEDULER_ENABLED,
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
        update_model(project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_ENABLED)

    def test_alter_scheduler_to_disabled(self, project):
        """Verify scheduler can be altered from ENABLE to DISABLE."""
        update_model(project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_DISABLED)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying UPDATE SCHEDULER to:", logs)
        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert_message_in_logs("alter dynamic table", logs)
        assert_message_in_logs("Applying REFRESH to:", logs)

    def test_alter_scheduler_to_enabled_with_target_lag(self, project):
        """Verify scheduler changes to ENABLE when only target_lag is added."""
        update_model(project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_DISABLED)
        run_dbt(["run"])

        update_model(project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_TARGET_LAG_ONLY)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying UPDATE SCHEDULER to:", logs)
        assert_message_in_logs("scheduler = 'ENABLE'", logs)
        assert_message_in_logs("target_lag = '2 minutes'", logs)
        assert_message_not_in_logs("Applying REFRESH to:", logs)

    def test_alter_scheduler_disabled_to_enabled_explicit(self, project):
        """Verify scheduler can be altered from DISABLE to ENABLE with explicit scheduler config."""
        update_model(project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_DISABLED)
        run_dbt(["run"])

        update_model(
            project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_DISABLED_TO_ENABLED
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying UPDATE SCHEDULER to:", logs)
        assert_message_in_logs("scheduler = 'ENABLE'", logs)
        assert_message_in_logs("target_lag = '2 minutes'", logs)
        assert_message_not_in_logs("Applying REFRESH to:", logs)

    def test_alter_scheduler_enabled_to_disabled_explicit(self, project):
        """Verify scheduler can be altered from ENABLE to DISABLE (reverse direction)."""
        update_model(
            project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_DISABLED_TO_ENABLED
        )
        run_dbt(["run"])

        update_model(project, "dynamic_table_scheduler", models.DYNAMIC_TABLE_SCHEDULER_DISABLED)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying UPDATE SCHEDULER to:", logs)
        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert_message_in_logs("alter dynamic table", logs)
        assert_message_in_logs("Applying REFRESH to:", logs)


class _SchedulerNoOpBase:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    def _assert_no_op_run_behavior(
        self, project, relation_name: str, expect_refresh: bool
    ) -> None:
        run_dbt(["seed"])
        run_dbt(["run"])

        initial_count = query_row_count(project, relation_name)
        assert initial_count == 3

        insert_record(project, "my_seed", {"id": 4, "value": 400})

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_in_logs("Applying REFRESH to:", logs, expect_refresh)

        refreshed_count = query_row_count(project, relation_name)
        if expect_refresh:
            assert refreshed_count == 4
        else:
            assert refreshed_count == 3


class TestSchedulerDisabledNoOpRefresh(_SchedulerNoOpBase):
    """
    Verify no-op runs still refresh disabled-scheduler dynamic tables when source data changes.
    """

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_scheduler_disabled_noop_refresh.sql": models.DYNAMIC_TABLE_SCHEDULER_DISABLED,
        }

    def test_no_op_run_refreshes_disabled_scheduler_dynamic_table(self, project):
        self._assert_no_op_run_behavior(
            project, "my_dt_scheduler_disabled_noop_refresh", expect_refresh=True
        )


class TestSchedulerDefaultDisabledNoOpRefresh(_SchedulerNoOpBase):
    """
    Verify no-op runs still refresh when scheduler defaults to disabled (no target_lag).
    """

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_no_target_lag_noop_refresh.sql": models.DYNAMIC_TABLE_NO_TARGET_LAG,
        }

    def test_no_op_run_refreshes_default_disabled_scheduler_dynamic_table(self, project):
        self._assert_no_op_run_behavior(
            project, "my_dt_no_target_lag_noop_refresh", expect_refresh=True
        )


class TestSchedulerEnabledNoOpNoRefresh:
    """
    Verify no-op runs do not issue explicit REFRESH when scheduler is enabled.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dt_scheduler_enabled_noop.sql": models.DYNAMIC_TABLE_SCHEDULER_ENABLED,
        }

    def test_no_op_run_does_not_refresh_enabled_scheduler_dynamic_table(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        insert_record(project, "my_seed", {"id": 4, "value": 400})

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_not_in_logs("Applying REFRESH to:", logs)


class TestIcebergSchedulerDisabled:
    """Verify SCHEDULER=DISABLE on a dynamic iceberg table."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_iceberg_dt_sched_disabled.sql": models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_DISABLED,
        }

    def test_iceberg_scheduler_disabled_in_create_ddl(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("create dynamic iceberg table", logs)
        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert_message_in_logs("Applying REFRESH to:", logs)
        assert query_relation_type(project, "my_iceberg_dt_sched_disabled") == "dynamic_table"


class TestIcebergSchedulerEnabled:
    """Verify SCHEDULER=ENABLE with target_lag on a dynamic iceberg table."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_iceberg_dt_sched_enabled.sql": models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_ENABLED,
        }

    def test_iceberg_scheduler_enabled_in_create_ddl(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("create dynamic iceberg table", logs)
        assert_message_in_logs("scheduler = 'ENABLE'", logs)
        assert_message_in_logs("target_lag = '2 minutes'", logs)
        assert_message_not_in_logs("Applying REFRESH to:", logs)
        assert query_relation_type(project, "my_iceberg_dt_sched_enabled") == "dynamic_table"


class TestIcebergSchedulerConfigChange:
    """Verify scheduler can be toggled on a dynamic iceberg table via ALTER."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_dt_scheduler.sql": models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_ENABLED,
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
            project, "iceberg_dt_scheduler", models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_ENABLED
        )

    def test_iceberg_alter_scheduler_to_disabled(self, project):
        """Verify scheduler can be altered from ENABLE to DISABLE on iceberg."""
        update_model(
            project, "iceberg_dt_scheduler", models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_DISABLED
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying UPDATE SCHEDULER to:", logs)
        assert_message_in_logs("scheduler = 'DISABLE'", logs)
        assert_message_in_logs("alter dynamic table", logs)
        assert_message_in_logs("Applying REFRESH to:", logs)

    def test_iceberg_alter_scheduler_to_enabled(self, project):
        """Verify scheduler can be altered from DISABLE to ENABLE on iceberg."""
        update_model(
            project, "iceberg_dt_scheduler", models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_DISABLED
        )
        run_dbt(["run"])

        update_model(
            project, "iceberg_dt_scheduler", models.DYNAMIC_ICEBERG_TABLE_SCHEDULER_ENABLED
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying UPDATE SCHEDULER to:", logs)
        assert_message_in_logs("scheduler = 'ENABLE'", logs)
        assert_message_in_logs("target_lag = '2 minutes'", logs)
        assert_message_not_in_logs("Applying REFRESH to:", logs)


class TestDynamicTableCopyGrants:
    """Test copy_grants functionality specifically for dynamic tables"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        copy_grants_model = """
{{ config(
    materialized='dynamic_table',
    target_lag='1 minute',
    snowflake_warehouse='DBT_TESTING_ALT',
    copy_grants=true
) }}

select * from {{ ref('my_seed') }}
"""
        yield {
            "my_dynamic_table_copy_grants.sql": copy_grants_model,
        }

    def test_dynamic_table_copy_grants_in_sql(self, project):
        """Test that copy_grants appears in the generated SQL on replace (full-refresh)"""
        run_dbt(["seed"])
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])

        assert_message_in_logs("copy grants", logs)
        assert query_relation_type(project, "my_dynamic_table_copy_grants") == "dynamic_table"
