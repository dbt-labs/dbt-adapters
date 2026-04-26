import os

import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import describe_dynamic_table, query_transient_status, update_model


# Get the alternate warehouse from environment, default to DBT_TESTING if not set
ALT_WAREHOUSE = os.getenv("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TESTING")


class Changes:
    iceberg: bool = False

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        my_models = {
            "dynamic_table_alter.sql": models.DYNAMIC_TABLE,
            "dynamic_table_replace.sql": models.DYNAMIC_TABLE,
        }
        if self.iceberg:
            my_models.update(
                {
                    "dynamic_table_iceberg_alter.sql": models.DYNAMIC_ICEBERG_TABLE,
                    "dynamic_table_iceberg_replace.sql": models.DYNAMIC_ICEBERG_TABLE,
                }
            )
        yield my_models

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

        update_model(project, "dynamic_table_alter", models.DYNAMIC_TABLE_ALTER)
        update_model(project, "dynamic_table_replace", models.DYNAMIC_TABLE_REPLACE)
        if self.iceberg:
            update_model(
                project, "dynamic_table_iceberg_alter", models.DYNAMIC_ICEBERG_TABLE_ALTER
            )
            update_model(
                project, "dynamic_table_iceberg_replace", models.DYNAMIC_ICEBERG_TABLE_REPLACE
            )

        yield

        update_model(project, "dynamic_table_alter", models.DYNAMIC_TABLE)
        update_model(project, "dynamic_table_replace", models.DYNAMIC_TABLE)
        if self.iceberg:
            update_model(project, "dynamic_table_iceberg_alter", models.DYNAMIC_ICEBERG_TABLE)
            update_model(project, "dynamic_table_iceberg_replace", models.DYNAMIC_ICEBERG_TABLE)

    def assert_changes_are_applied(self, project):
        altered = describe_dynamic_table(project, "dynamic_table_alter")
        assert altered.snowflake_warehouse == "DBT_TESTING"
        assert altered.target_lag == "5 minutes"  # this updated
        assert altered.refresh_mode == "INCREMENTAL"

        replaced = describe_dynamic_table(project, "dynamic_table_replace")
        assert replaced.snowflake_warehouse == "DBT_TESTING"
        assert replaced.target_lag == "2 minutes"
        assert replaced.refresh_mode == "FULL"  # this updated

        if self.iceberg:
            altered_iceberg = describe_dynamic_table(project, "dynamic_table_iceberg_alter")
            assert altered_iceberg.snowflake_warehouse == "DBT_TESTING"
            assert altered_iceberg.target_lag == "5 minutes"  # this updated
            assert altered_iceberg.refresh_mode == "INCREMENTAL"

            replaced_iceberg = describe_dynamic_table(project, "dynamic_table_iceberg_replace")
            assert replaced_iceberg.snowflake_warehouse == "DBT_TESTING"
            assert replaced_iceberg.target_lag == "2 minutes"
            assert replaced_iceberg.refresh_mode == "FULL"  # this updated

    def assert_changes_are_not_applied(self, project):
        altered = describe_dynamic_table(project, "dynamic_table_alter")
        assert altered.snowflake_warehouse == "DBT_TESTING"
        assert altered.target_lag == "2 minutes"  # this would have updated, but didn't
        assert altered.refresh_mode == "INCREMENTAL"

        replaced = describe_dynamic_table(project, "dynamic_table_replace")
        assert replaced.snowflake_warehouse == "DBT_TESTING"
        assert replaced.target_lag == "2 minutes"
        assert replaced.refresh_mode == "INCREMENTAL"  # this would have updated, but didn't

        if self.iceberg:
            altered_iceberg = describe_dynamic_table(project, "dynamic_table_iceberg_alter")
            assert altered_iceberg.snowflake_warehouse == "DBT_TESTING"
            assert altered_iceberg.target_lag == "2 minutes"  # this would have updated, but didn't
            assert altered_iceberg.refresh_mode == "INCREMENTAL"

            replaced_iceberg = describe_dynamic_table(project, "dynamic_table_iceberg_replace")
            assert replaced_iceberg.snowflake_warehouse == "DBT_TESTING"
            assert replaced_iceberg.target_lag == "2 minutes"
            assert (
                replaced_iceberg.refresh_mode == "INCREMENTAL"
            )  # this would have updated, but didn't

    def test_full_refresh_is_always_successful(self, project):
        # this always passes and always changes the configuration, regardless of on_configuration_change
        # and regardless of whether the changes require a replace versus an alter
        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])
        self.assert_changes_are_applied(project)
        assert_message_in_logs("create or replace dynamic table", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)


class TestChangesApply(Changes):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    def test_changes_are_applied(self, project):
        # this passes and changes the configuration
        _, logs = run_dbt_and_capture(["--debug", "run"])
        self.assert_changes_are_applied(project)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs)
        assert_message_in_logs("create or alter dynamic table", logs)
        assert_message_in_logs("create or replace dynamic table", logs, expected_pass=False)


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


class TestInitializationWarehouseChanges:
    """Tests for snowflake_initialization_warehouse configuration changes."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_init_wh.sql": models.DYNAMIC_TABLE_WITH_INIT_WAREHOUSE,
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
        # Create initial dynamic table with initialization_warehouse
        run_dbt(["run", "--full-refresh"])
        yield
        # Reset model to original state
        update_model(project, "dynamic_table_init_wh", models.DYNAMIC_TABLE_WITH_INIT_WAREHOUSE)

    def test_create_with_initialization_warehouse(self, project):
        """Verify dynamic table is created with initialization_warehouse set."""
        dt = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt.snowflake_warehouse == "DBT_TESTING"
        # Uses ALT_WAREHOUSE - can be different from snowflake_warehouse when env var is set
        assert dt.snowflake_initialization_warehouse == ALT_WAREHOUSE

    def test_alter_initialization_warehouse(self, project):
        """Verify initialization_warehouse can be altered to a different value."""
        # Initial state - uses ALT_WAREHOUSE
        dt_before = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_before.snowflake_initialization_warehouse == ALT_WAREHOUSE

        # Update model (changes initialization_warehouse from ALT_WAREHOUSE to DBT_TESTING)
        update_model(
            project, "dynamic_table_init_wh", models.DYNAMIC_TABLE_WITH_INIT_WAREHOUSE_ALTER
        )
        run_dbt(["run"])

        # Verify initialization_warehouse was changed
        dt_after = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_after.snowflake_initialization_warehouse == "DBT_TESTING"

    def test_unset_initialization_warehouse(self, project):
        """Verify initialization_warehouse can be unset (removed)."""
        # Initial state - has initialization_warehouse
        dt_before = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_before.snowflake_initialization_warehouse == ALT_WAREHOUSE

        # Update to remove initialization_warehouse
        update_model(project, "dynamic_table_init_wh", models.DYNAMIC_TABLE_WITHOUT_INIT_WAREHOUSE)
        run_dbt(["run"])

        # Verify initialization_warehouse was unset
        dt_after = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_after.snowflake_initialization_warehouse is None


class TestImmutableWhereChanges:
    """Tests for immutable_where configuration changes."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_immutable.sql": models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE,
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
        # Create initial dynamic table with immutable_where
        run_dbt(["run", "--full-refresh"])
        yield
        # Reset model to original state
        update_model(project, "dynamic_table_immutable", models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE)

    def test_create_with_immutable_where(self, project):
        """Verify dynamic table is created with immutable_where set."""
        dt = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt.immutable_where == "id < 100"

    def test_alter_immutable_where(self, project):
        """Verify immutable_where can be altered."""
        # Initial state
        dt_before = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt_before.immutable_where == "id < 100"

        # Update to new immutable_where
        update_model(
            project, "dynamic_table_immutable", models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_ALTER
        )
        run_dbt(["run"])

        # Verify change was applied
        dt_after = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt_after.immutable_where == "id < 50"

    def test_unset_immutable_where(self, project):
        """Verify immutable_where can be unset (removed)."""
        # Initial state - has immutable_where
        dt_before = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt_before.immutable_where == "id < 100"

        # Update to remove immutable_where
        update_model(
            project, "dynamic_table_immutable", models.DYNAMIC_TABLE_WITHOUT_IMMUTABLE_WHERE
        )
        run_dbt(["run"])

        # Verify immutable_where was unset
        dt_after = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt_after.immutable_where is None

    def test_alter_immutable_where_with_other_changes(self, project):
        """Verify immutable_where changes alongside other config changes don't cause a syntax error.

        Snowflake does not allow IMMUTABLE WHERE in the same SET clause as other options
        (e.g. TARGET_LAG). Each must be issued as a separate ALTER statement.
        """
        # Initial state
        dt_before = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt_before.immutable_where == "id < 100"
        assert dt_before.target_lag == "2 minutes"

        # Update both immutable_where and target_lag simultaneously
        update_model(
            project,
            "dynamic_table_immutable",
            models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_AND_LAG_ALTER,
        )
        run_dbt(["run"])

        # Verify both changes were applied
        dt_after = describe_dynamic_table(project, "dynamic_table_immutable")
        assert dt_after.immutable_where == "id < 50"
        assert dt_after.target_lag == "5 minutes"


class TestImmutableWhereWithClusterByChanges:
    """Tests for the immutable_where and cluster_by ALTER statements being applied simultaneously."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_imw_cluster.sql": models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_NO_CLUSTER,
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
            "dynamic_table_imw_cluster",
            models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_NO_CLUSTER,
        )

    def test_alter_immutable_where_and_cluster_by_simultaneously(self, project):
        """Verify immutable_where and cluster_by can be altered simultaneously."""
        dt_before = describe_dynamic_table(project, "dynamic_table_imw_cluster")
        assert dt_before.immutable_where == "id < 100"
        assert dt_before.cluster_by is None

        update_model(
            project,
            "dynamic_table_imw_cluster",
            models.DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_AND_CLUSTER_ALTER,
        )
        run_dbt(["run"])

        dt_after = describe_dynamic_table(project, "dynamic_table_imw_cluster")
        assert dt_after.immutable_where == "id < 50"
        assert dt_after.cluster_by is not None


class TestClusterByChanges:
    """Tests for cluster_by configuration changes."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_cluster.sql": models.DYNAMIC_TABLE_WITH_CLUSTER_BY,
            "dynamic_table_cluster_multi.sql": models.DYNAMIC_TABLE_WITH_CLUSTER_BY_MULTI,
            "dynamic_table_no_cluster.sql": models.DYNAMIC_TABLE_WITHOUT_CLUSTER_BY,
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
        # Create initial dynamic tables
        run_dbt(["run", "--full-refresh"])
        yield
        # Reset models to original state
        update_model(project, "dynamic_table_cluster", models.DYNAMIC_TABLE_WITH_CLUSTER_BY)
        update_model(
            project, "dynamic_table_cluster_multi", models.DYNAMIC_TABLE_WITH_CLUSTER_BY_MULTI
        )
        update_model(project, "dynamic_table_no_cluster", models.DYNAMIC_TABLE_WITHOUT_CLUSTER_BY)

    def test_create_with_cluster_by(self, project):
        """Verify dynamic table is created with single column cluster_by."""
        dt = describe_dynamic_table(project, "dynamic_table_cluster")
        # Snowflake returns cluster_by in a specific format, typically with LINEAR prefix
        assert dt.cluster_by is not None
        assert "ID" in dt.cluster_by.upper()

    def test_create_with_cluster_by_multi_column(self, project):
        """Verify dynamic table is created with multi-column cluster_by."""
        dt = describe_dynamic_table(project, "dynamic_table_cluster_multi")
        assert dt.cluster_by is not None
        cluster_by_upper = dt.cluster_by.upper()
        assert "ID" in cluster_by_upper
        assert "VALUE" in cluster_by_upper

    def test_alter_cluster_by(self, project):
        """Verify cluster_by can be altered to a different column."""
        # Initial state - clustered by 'id'
        dt_before = describe_dynamic_table(project, "dynamic_table_cluster")
        assert dt_before.cluster_by is not None
        assert "ID" in dt_before.cluster_by.upper()

        # Update to cluster by 'value' instead
        update_model(project, "dynamic_table_cluster", models.DYNAMIC_TABLE_WITH_CLUSTER_BY_ALTER)
        run_dbt(["run"])

        # Verify cluster_by was changed
        dt_after = describe_dynamic_table(project, "dynamic_table_cluster")
        assert dt_after.cluster_by is not None
        assert "VALUE" in dt_after.cluster_by.upper()

    def test_drop_cluster_by(self, project):
        """Verify cluster_by can be removed (DROP CLUSTERING KEY)."""
        # Initial state - has cluster_by
        dt_before = describe_dynamic_table(project, "dynamic_table_cluster")
        assert dt_before.cluster_by is not None

        # Update to remove cluster_by
        update_model(project, "dynamic_table_cluster", models.DYNAMIC_TABLE_WITHOUT_CLUSTER_BY)
        run_dbt(["run"])

        # Verify cluster_by was removed
        dt_after = describe_dynamic_table(project, "dynamic_table_cluster")
        assert dt_after.cluster_by is None

    def test_add_cluster_by(self, project):
        """Verify cluster_by can be added to a table that didn't have one."""
        # Initial state - no cluster_by
        dt_before = describe_dynamic_table(project, "dynamic_table_no_cluster")
        assert dt_before.cluster_by is None

        # Update to add cluster_by
        update_model(project, "dynamic_table_no_cluster", models.DYNAMIC_TABLE_WITH_CLUSTER_BY)
        run_dbt(["run"])

        # Verify cluster_by was added
        dt_after = describe_dynamic_table(project, "dynamic_table_no_cluster")
        assert dt_after.cluster_by is not None
        assert "ID" in dt_after.cluster_by.upper()


class TestClusterByNoneColumnName:
    """Test clustering by a column literally named 'NONE' to ensure we don't
    incorrectly normalize it to Python None."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed_none.csv": models.SEED_WITH_NONE_COLUMN}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_none_col.sql": models.DYNAMIC_TABLE_WITH_CLUSTER_BY_NONE_COLUMN,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_cluster_by_column_named_none(self, project):
        """Verify we can cluster by a column literally named 'NONE'."""
        run_dbt(["run", "--full-refresh"])

        dt = describe_dynamic_table(project, "dynamic_table_none_col")
        # cluster_by should NOT be None - it should contain the column name "NONE"
        assert (
            dt.cluster_by is not None
        ), "cluster_by should not be Python None when clustering by a column named 'NONE'"
        assert "NONE" in dt.cluster_by.upper()


class TestTransientChanges:
    """Tests for transient configuration changes on dynamic tables."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_transient.sql": models.DYNAMIC_TABLE_TRANSIENT,
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
        # Create initial dynamic table with transient=True
        run_dbt(["run", "--full-refresh"])
        yield
        # Reset model to original state
        update_model(project, "dynamic_table_transient", models.DYNAMIC_TABLE_TRANSIENT)

    def test_create_transient_dynamic_table(self, project):
        """Verify dynamic table is created with transient=True."""
        assert query_transient_status(project, "dynamic_table_transient") is True

    def test_change_transient_to_non_transient_requires_full_refresh(self, project):
        """Verify changing transient from True to False triggers full refresh (table recreation)."""
        assert query_transient_status(project, "dynamic_table_transient") is True

        # Update to non-transient
        update_model(project, "dynamic_table_transient", models.DYNAMIC_TABLE_NON_TRANSIENT)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        # Verify transient was changed via CREATE OR REPLACE (not CREATE OR ALTER)
        assert query_transient_status(project, "dynamic_table_transient") is False
        assert_message_in_logs("create or replace dynamic table", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)


class TestNonTransientChanges:
    """Tests for creating non-transient dynamic tables and changing to transient."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_non_transient.sql": models.DYNAMIC_TABLE_NON_TRANSIENT,
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
        # Create initial dynamic table with transient=False
        run_dbt(["run", "--full-refresh"])
        yield
        # Reset model to original state
        update_model(project, "dynamic_table_non_transient", models.DYNAMIC_TABLE_NON_TRANSIENT)

    def test_create_non_transient_dynamic_table(self, project):
        """Verify dynamic table is created with transient=False."""
        assert query_transient_status(project, "dynamic_table_non_transient") is False

    def test_change_non_transient_to_transient_requires_full_refresh(self, project):
        """Verify changing transient from False to True triggers full refresh."""
        assert query_transient_status(project, "dynamic_table_non_transient") is False

        # Update to transient
        update_model(project, "dynamic_table_non_transient", models.DYNAMIC_TABLE_TRANSIENT)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        # Verify transient was changed via CREATE OR REPLACE (not CREATE OR ALTER)
        assert query_transient_status(project, "dynamic_table_non_transient") is True
        assert_message_in_logs("create or replace transient dynamic table", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)


class TestTransientBehaviorFlagDisabled:
    """Tests for default transient behavior when behavior flag is disabled (default)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_default.sql": models.DYNAMIC_TABLE_DEFAULT_TRANSIENT,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Behavior flag disabled (default) - dynamic tables should NOT be transient by default
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_default_is_non_transient_when_flag_disabled(self, project):
        """When behavior flag is disabled, dynamic tables default to non-transient."""
        run_dbt(["run", "--full-refresh"])
        assert query_transient_status(project, "dynamic_table_default") is False


class TestTransientBehaviorFlagEnabled:
    """Tests for default transient behavior when behavior flag is enabled."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_default.sql": models.DYNAMIC_TABLE_DEFAULT_TRANSIENT,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Enable behavior flag - dynamic tables should be transient by default
        return {
            "flags": {"snowflake_default_transient_dynamic_tables": True},
            "models": {"on_configuration_change": "apply"},
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_default_is_transient_when_flag_enabled(self, project):
        """When behavior flag is enabled, dynamic tables default to transient."""
        run_dbt(["run", "--full-refresh"])
        assert query_transient_status(project, "dynamic_table_default") is True


class TestNoTransientConfigDoesNotRecreateTransientTable:
    """
    When a transient dynamic table already exists and the model has no explicit
    transient config, a subsequent `dbt run` must NOT recreate the table --
    regardless of the behavior flag setting.

    This verifies that absence of the transient parameter means "don't care"
    rather than "apply the flag default and force convergence".
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_default.sql": models.DYNAMIC_TABLE_TRANSIENT,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_transient_table_not_recreated_when_config_absent(self, project):
        """Existing transient table should survive a run when model omits transient config."""
        # Create the table as transient (explicit config)
        run_dbt(["run", "--full-refresh"])
        assert query_transient_status(project, "dynamic_table_default") is True

        # Switch model to have NO explicit transient config
        update_model(project, "dynamic_table_default", models.DYNAMIC_TABLE_DEFAULT_TRANSIENT)

        # Run without --full-refresh; should detect no transient change
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("create or replace", logs, False)

        # Table should still be transient -- untouched
        assert query_transient_status(project, "dynamic_table_default") is True


class TestNoTransientConfigDoesNotRecreateNonTransientTable:
    """
    When a non-transient dynamic table already exists, the behavior flag is ON
    (default=transient), and the model has no explicit transient config, a
    subsequent `dbt run` must NOT recreate the table.

    This is the more subtle case: the flag says "default to transient" but the
    existing table is non-transient. Because the user didn't explicitly request
    transient, we should leave the table alone.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dynamic_table_default.sql": models.DYNAMIC_TABLE_NON_TRANSIENT,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"snowflake_default_transient_dynamic_tables": True},
            "models": {"on_configuration_change": "apply"},
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_non_transient_table_not_recreated_when_flag_on_and_config_absent(self, project):
        """Existing non-transient table should survive when flag=ON but model omits transient."""
        # Create the table as non-transient (explicit config)
        run_dbt(["run", "--full-refresh"])
        assert query_transient_status(project, "dynamic_table_default") is False

        # Switch model to have NO explicit transient config
        update_model(project, "dynamic_table_default", models.DYNAMIC_TABLE_DEFAULT_TRANSIENT)

        # Run without --full-refresh; should detect no transient change
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("create or replace", logs, False)

        # Table should still be non-transient -- untouched despite flag=ON
        assert query_transient_status(project, "dynamic_table_default") is False


class TestSqlOnlyChangeIsNoop:
    """Changing only the SQL body (query) must NOT trigger CREATE OR ALTER
    because configuration change detection does not compare the SQL definition."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_sql_only_change.sql": models.DYNAMIC_TABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_sql_only_change_is_noop(self, project):
        fqn = f"{project.database}.{project.test_schema}.dt_sql_only_change"
        run_dbt(["run", "--full-refresh"])

        # change only the SQL, not the config
        update_model(project, "dt_sql_only_change", models.DYNAMIC_TABLE_EXTRA_COLUMN)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)
        assert_message_in_logs(f"create or replace dynamic table {fqn}", logs, expected_pass=False)


class _NoChangeIsNoopBase:
    """Re-running with identical config must NOT trigger CREATE OR ALTER.

    Subclasses set MODEL_SQL to exercise different cluster_by shapes and catch
    format mismatches between dbt config and SHOW DYNAMIC TABLES output
    (e.g. Snowflake wrapping simple columns with LINEAR(...)).
    """

    MODEL_SQL: str  # override in subclasses

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_no_change.sql": self.MODEL_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "apply"}}

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def _assert_no_change(self, dbt_command):
        run_dbt(["run", "--full-refresh"])
        _, logs = run_dbt_and_capture(["--debug"] + dbt_command)

        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)

    def test_no_change_run_is_noop(self, project):
        self._assert_no_change(["run"])

    def test_no_change_build_is_noop(self, project):
        self._assert_no_change(["build"])


class TestNoChangeFullConfig(_NoChangeIsNoopBase):
    """Full config with cluster_by=["HASH(id)", "id"], initialization_warehouse, scheduler."""

    MODEL_SQL = models.DYNAMIC_TABLE_FULL_CONFIG


class TestNoChangeClusterBySingleColumn(_NoChangeIsNoopBase):
    """Single plain column cluster_by — Snowflake wraps it with LINEAR(...)."""

    MODEL_SQL = models.DYNAMIC_TABLE_CLUSTER_BY_SINGLE


class TestNoChangeClusterByTwoColumns(_NoChangeIsNoopBase):
    """Two plain columns cluster_by — Snowflake wraps them with LINEAR(...)."""

    MODEL_SQL = models.DYNAMIC_TABLE_CLUSTER_BY_TWO_COLUMNS


class TestIcebergInitializationWarehouseChanges:
    """Tests for initialization_warehouse ALTER on dynamic iceberg tables."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_init_wh.sql": models.DYNAMIC_ICEBERG_TABLE_WITH_INIT_WAREHOUSE,
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
        update_model(project, "iceberg_init_wh", models.DYNAMIC_ICEBERG_TABLE_WITH_INIT_WAREHOUSE)

    def test_iceberg_create_with_initialization_warehouse(self, project):
        dt = describe_dynamic_table(project, "iceberg_init_wh")
        assert dt.snowflake_initialization_warehouse == ALT_WAREHOUSE

    def test_iceberg_unset_initialization_warehouse(self, project):
        dt_before = describe_dynamic_table(project, "iceberg_init_wh")
        assert dt_before.snowflake_initialization_warehouse == ALT_WAREHOUSE

        # Remove initialization_warehouse
        update_model(
            project, "iceberg_init_wh", models.DYNAMIC_ICEBERG_TABLE_WITHOUT_INIT_WAREHOUSE
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_in_logs("alter dynamic table", logs)
        dt_after = describe_dynamic_table(project, "iceberg_init_wh")
        assert dt_after.snowflake_initialization_warehouse is None


class TestIcebergImmutableWhereChanges:
    """Tests for immutable_where ALTER on dynamic iceberg tables."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_immutable.sql": models.DYNAMIC_ICEBERG_TABLE_WITH_IMMUTABLE_WHERE,
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
            project, "iceberg_immutable", models.DYNAMIC_ICEBERG_TABLE_WITH_IMMUTABLE_WHERE
        )

    def test_iceberg_create_with_immutable_where(self, project):
        dt = describe_dynamic_table(project, "iceberg_immutable")
        assert dt.immutable_where == "id < 100"

    def test_iceberg_alter_immutable_where(self, project):
        # Change immutable_where from 'id < 100' to 'id < 50'
        update_model(
            project, "iceberg_immutable", models.DYNAMIC_ICEBERG_TABLE_WITH_IMMUTABLE_WHERE_ALTER
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        dt_after = describe_dynamic_table(project, "iceberg_immutable")
        assert dt_after.immutable_where == "id < 50"

    def test_iceberg_unset_immutable_where(self, project):
        # Remove immutable_where entirely
        update_model(
            project, "iceberg_immutable", models.DYNAMIC_ICEBERG_TABLE_WITHOUT_IMMUTABLE_WHERE
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        dt_after = describe_dynamic_table(project, "iceberg_immutable")
        assert dt_after.immutable_where is None


CREATE_ROW_ACCESS_POLICY = """
create or replace row access policy always_true as (id integer) returns boolean ->
  case
      when id = 1 then true
      else false
  end
;
"""

CREATE_TAG = """
create or replace tag tag_name COMMENT = 'testing'
"""


class TestRowAccessPolicyWithCreateOrAlter:
    """Tests that CREATE OR ALTER succeeds when row_access_policy is configured.

    Validates that:
    - A config change (target_lag) on a DT with row_access_policy triggers CREATE OR ALTER
    - The CREATE OR ALTER DDL omits the policy (Snowflake error 001506) and succeeds
    - A policy-only change (no other config change) is a no-op (known limitation)
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, project):
        project.run_sql(CREATE_ROW_ACCESS_POLICY)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dt_with_policy.sql": models.DYNAMIC_TABLE_WITH_ROW_ACCESS_POLICY,
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
        update_model(project, "dt_with_policy", models.DYNAMIC_TABLE_WITH_ROW_ACCESS_POLICY)

    def test_config_change_with_policy_uses_create_or_alter(self, project):
        """Changing target_lag on a DT with row_access_policy should use CREATE OR ALTER.

        Snowflake error 001506: CREATE OR ALTER does not support setting policies or tags.
        The CREATE OR ALTER DDL must omit row_access_policy/table_tag clauses.
        The policy remains attached to the table from the initial CREATE.
        """
        dt_before = describe_dynamic_table(project, "dt_with_policy")
        assert dt_before.target_lag == "2 minutes"

        update_model(project, "dt_with_policy", models.DYNAMIC_TABLE_WITH_ROW_ACCESS_POLICY_ALTER)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying CREATE OR ALTER to:", logs)
        assert_message_in_logs("create or alter dynamic table", logs)
        assert_message_in_logs("with row access policy", logs, expected_pass=False)

        dt_after = describe_dynamic_table(project, "dt_with_policy")
        assert dt_after.target_lag == "5 minutes"

    def test_policy_only_change_is_noop(self, project):
        """Removing row_access_policy (with no other config change) does NOT trigger a rebuild.

        TODO: row_access_policy is not tracked by SnowflakeDynamicTableConfigChangeset and
        SHOW DYNAMIC TABLES does not return policy information, so dbt cannot detect
        policy-only changes. Changing row_access_policy requires --full-refresh.
        This limitation applies to all relation types (tables, views, dynamic tables).
        """
        update_model(project, "dt_with_policy", models.DYNAMIC_TABLE_WITHOUT_ROW_ACCESS_POLICY)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)


class TestTableTagWithCreateOrAlter:
    """Tests that CREATE OR ALTER succeeds when table_tag is configured."""

    @pytest.fixture(scope="class", autouse=True)
    def setup_tag(self, project):
        project.run_sql(CREATE_TAG)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dt_with_tag.sql": models.DYNAMIC_TABLE_WITH_TAG,
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
        update_model(project, "dt_with_tag", models.DYNAMIC_TABLE_WITH_TAG)

    def test_config_change_with_tag_uses_create_or_alter(self, project):
        """Changing target_lag on a DT with table_tag should use CREATE OR ALTER.

        Snowflake error 001506: CREATE OR ALTER does not support setting policies or tags.
        The CREATE OR ALTER DDL must omit table_tag clauses.
        The tag remains attached to the table from the initial CREATE.
        """
        dt_before = describe_dynamic_table(project, "dt_with_tag")
        assert dt_before.target_lag == "2 minutes"

        update_model(project, "dt_with_tag", models.DYNAMIC_TABLE_WITH_TAG_ALTER)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying CREATE OR ALTER to:", logs)
        assert_message_in_logs("create or alter dynamic table", logs)
        assert_message_in_logs("with tag", logs, expected_pass=False)

        dt_after = describe_dynamic_table(project, "dt_with_tag")
        assert dt_after.target_lag == "5 minutes"

    def test_tag_only_change_is_noop(self, project):
        """Removing table_tag (with no other config change) does NOT trigger a rebuild.

        TODO: table_tag is not tracked by SnowflakeDynamicTableConfigChangeset and
        SHOW DYNAMIC TABLES does not return tag information, so dbt cannot detect
        tag-only changes. Changing table_tag requires --full-refresh.
        This limitation applies to all relation types (tables, views, dynamic tables).
        """
        update_model(project, "dt_with_tag", models.DYNAMIC_TABLE_WITHOUT_TAG)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("No configuration changes were identified on:", logs)
        assert_message_in_logs("Applying CREATE OR ALTER to:", logs, expected_pass=False)


class TestIcebergClusterByChanges:
    """Tests for cluster_by ALTER on dynamic iceberg tables."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_cluster.sql": models.DYNAMIC_ICEBERG_TABLE_WITH_CLUSTER_BY,
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
        update_model(project, "iceberg_cluster", models.DYNAMIC_ICEBERG_TABLE_WITH_CLUSTER_BY)

    def test_iceberg_create_with_cluster_by(self, project):
        dt = describe_dynamic_table(project, "iceberg_cluster")
        assert dt.cluster_by == "id"

    def test_iceberg_alter_cluster_by(self, project):
        # Change cluster_by from 'id' to 'value'
        update_model(
            project, "iceberg_cluster", models.DYNAMIC_ICEBERG_TABLE_WITH_CLUSTER_BY_ALTER
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        dt_after = describe_dynamic_table(project, "iceberg_cluster")
        assert dt_after.cluster_by == "value"

    def test_iceberg_drop_cluster_by(self, project):
        # Remove cluster_by entirely
        update_model(project, "iceberg_cluster", models.DYNAMIC_ICEBERG_TABLE_WITHOUT_CLUSTER_BY)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        dt_after = describe_dynamic_table(project, "iceberg_cluster")
        assert dt_after.cluster_by is None


class TestIcebergRefreshModeChangeTriggersReplace:
    """Tests that changing refresh_mode on an Iceberg DT triggers CREATE OR REPLACE.

    Snowflake's ALTER DYNAMIC TABLE ... SET does not support refresh_mode,
    so the Iceberg ALTER path falls back to CREATE OR REPLACE when refresh_mode changes.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_refresh.sql": models.DYNAMIC_ICEBERG_TABLE,
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
        update_model(project, "iceberg_refresh", models.DYNAMIC_ICEBERG_TABLE)

    def test_refresh_mode_change_triggers_replace(self, project):
        """Changing refresh_mode on an Iceberg DT should fall back to CREATE OR REPLACE."""
        dt_before = describe_dynamic_table(project, "iceberg_refresh")
        assert dt_before.refresh_mode == "INCREMENTAL"

        update_model(project, "iceberg_refresh", models.DYNAMIC_ICEBERG_TABLE_REPLACE)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("refresh_mode cannot be altered on Iceberg tables", logs)
        assert_message_in_logs("create or replace dynamic iceberg table", logs)
        assert_message_in_logs("Applying ALTER to:", logs, expected_pass=False)

        dt_after = describe_dynamic_table(project, "iceberg_refresh")
        assert dt_after.refresh_mode == "FULL"


class TestSqlAndConfigChangeAppliesBoth:
    """Tests that simultaneous SQL + config change applies both via CREATE OR ALTER.

    Validates that when a dynamic table's SQL definition and a tracked
    configuration field (target_lag) change in the same run, CREATE OR ALTER
    is used and both the new SQL and the new config take effect.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dt_sql_and_config.sql": models.DYNAMIC_TABLE,
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
        update_model(project, "dt_sql_and_config", models.DYNAMIC_TABLE)

    def test_sql_and_config_change_applies_both(self, project):
        dt_before = describe_dynamic_table(project, "dt_sql_and_config")
        assert dt_before.target_lag == "2 minutes"

        update_model(
            project, "dt_sql_and_config", models.DYNAMIC_TABLE_EXTRA_COLUMN_TARGET_LAG_FIVE
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying CREATE OR ALTER to:", logs)
        assert_message_in_logs("create or alter dynamic table", logs)

        dt_after = describe_dynamic_table(project, "dt_sql_and_config")
        assert dt_after.target_lag == "5 minutes"

        columns = project.run_sql(
            f"select column_name from information_schema.columns "
            f"where table_schema = upper('{project.test_schema}') "
            f"and table_name = upper('dt_sql_and_config')",
            fetch="all",
        )
        column_names = {row[0].upper() for row in columns}
        assert "EXTRA_COL" in column_names


class TestIcebergTargetLagChangeUsesAlter:
    """Tests that target_lag change on Iceberg DT uses ALTER SET (not REPLACE)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_lag.sql": models.DYNAMIC_ICEBERG_TABLE,
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
        update_model(project, "iceberg_lag", models.DYNAMIC_ICEBERG_TABLE)

    def test_iceberg_target_lag_change_uses_alter(self, project):
        dt_before = describe_dynamic_table(project, "iceberg_lag")
        assert dt_before.target_lag == "2 minutes"

        update_model(project, "iceberg_lag", models.DYNAMIC_ICEBERG_TABLE_ALTER)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_in_logs("alter dynamic table", logs)
        assert_message_in_logs(
            "create or replace dynamic iceberg table", logs, expected_pass=False
        )

        dt_after = describe_dynamic_table(project, "iceberg_lag")
        assert dt_after.target_lag == "5 minutes"


class TestWarehouseChangeAppliesAlter:
    """Tests that snowflake_warehouse change is applied via CREATE OR ALTER (INFO_SCHEMA).

    Skipped gracefully if SNOWFLAKE_TEST_ALT_WAREHOUSE is not distinct from DBT_TESTING.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "dt_warehouse.sql": models.DYNAMIC_TABLE,
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
        update_model(project, "dt_warehouse", models.DYNAMIC_TABLE)

    def test_warehouse_change_applies_alter(self, project):
        if ALT_WAREHOUSE.upper() == "DBT_TESTING":
            pytest.skip(
                "SNOWFLAKE_TEST_ALT_WAREHOUSE is not distinct from DBT_TESTING; "
                "cannot validate warehouse change."
            )

        dt_before = describe_dynamic_table(project, "dt_warehouse")
        assert dt_before.snowflake_warehouse == "DBT_TESTING"

        update_model(project, "dt_warehouse", models.DYNAMIC_TABLE_ALT_WAREHOUSE)
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying CREATE OR ALTER to:", logs)
        assert_message_in_logs("create or alter dynamic table", logs)

        dt_after = describe_dynamic_table(project, "dt_warehouse")
        assert dt_after.snowflake_warehouse.upper() == ALT_WAREHOUSE.upper()


class TestIcebergInitWarehouseAlter:
    """Tests that Iceberg initialization_warehouse value change uses ALTER SET."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "iceberg_init_wh_alter.sql": models.DYNAMIC_ICEBERG_TABLE_WITH_INIT_WAREHOUSE,
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
            project, "iceberg_init_wh_alter", models.DYNAMIC_ICEBERG_TABLE_WITH_INIT_WAREHOUSE
        )

    def test_iceberg_init_warehouse_value_change(self, project):
        if ALT_WAREHOUSE.upper() == "DBT_TESTING":
            pytest.skip(
                "SNOWFLAKE_TEST_ALT_WAREHOUSE is not distinct from DBT_TESTING; "
                "cannot validate initialization_warehouse value change."
            )

        dt_before = describe_dynamic_table(project, "iceberg_init_wh_alter")
        assert dt_before.snowflake_initialization_warehouse == ALT_WAREHOUSE

        update_model(
            project,
            "iceberg_init_wh_alter",
            models.DYNAMIC_ICEBERG_TABLE_WITH_INIT_WAREHOUSE_ALTER,
        )
        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert_message_in_logs("Applying ALTER to:", logs)
        assert_message_in_logs("alter dynamic table", logs)

        dt_after = describe_dynamic_table(project, "iceberg_init_wh_alter")
        assert dt_after.snowflake_initialization_warehouse == "DBT_TESTING"
