import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import query_relation_type, update_model


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


class TestDynamicTableCopyGrants:

    DT_COPY_GRANTS = "my_dynamic_table_copy_grants"
    DT_NO_COPY_GRANTS = "my_dynamic_table_no_copy_grants"

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{self.DT_COPY_GRANTS}.sql": models.DYNAMIC_TABLE_COPY_GRANTS,
            f"{self.DT_NO_COPY_GRANTS}.sql": models.DYNAMIC_TABLE_NO_COPY_GRANTS,
        }

    def test_copy_grants_in_replace_ddl(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])

        copy_grants_qualified = (
            f"{project.database}.{project.test_schema}.{self.DT_COPY_GRANTS}"
        )
        no_copy_grants_qualified = (
            f"{project.database}.{project.test_schema}.{self.DT_NO_COPY_GRANTS}"
        )

        assert_message_in_logs(
            f"create or replace dynamic table {copy_grants_qualified}", logs
        )
        assert_message_in_logs("copy grants", logs)

        assert_message_in_logs(
            f"create or replace dynamic table {no_copy_grants_qualified}", logs
        )


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

        assert_message_in_logs(f"create dynamic table {model_qualified_name}", logs, False)
        assert_message_in_logs(
            f"create or replace dynamic table {model_qualified_name}", logs, False
        )
        assert_message_in_logs("refresh_mode = AUTO", logs, False)
        assert_message_in_logs(
            f"No configuration changes were identified on: `{model_qualified_name}`. Continuing.",
            logs,
        )


class TestSchedulerDisabled:
    """Verify SCHEDULER=DISABLED appears in DDL and REFRESH is called after build."""

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

        model_qualified = (
            f"{project.database}.{project.test_schema}.MY_DT_SCHEDULER_DISABLED"
        )
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
        assert_message_in_logs("Applying REFRESH to:", logs, False)
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
        assert_message_in_logs("Applying REFRESH to:", logs, False)
