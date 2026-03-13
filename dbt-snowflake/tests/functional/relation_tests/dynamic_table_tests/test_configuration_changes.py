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
        run_dbt(["run"])

        # Verify transient was changed (requires full refresh/recreation)
        assert query_transient_status(project, "dynamic_table_transient") is False


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
        run_dbt(["run"])

        # Verify transient was changed (requires full refresh/recreation)
        assert query_transient_status(project, "dynamic_table_non_transient") is True


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
