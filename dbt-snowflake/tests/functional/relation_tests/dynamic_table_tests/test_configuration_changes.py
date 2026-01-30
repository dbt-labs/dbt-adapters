import pytest

from dbt.tests.util import run_dbt

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import describe_dynamic_table, update_model


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
        assert dt.snowflake_initialization_warehouse == "DBT_TESTING_ALT"

    def test_alter_initialization_warehouse(self, project):
        """Verify initialization_warehouse can be altered."""
        # Initial state
        dt_before = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_before.snowflake_initialization_warehouse == "DBT_TESTING_ALT"

        # Update to new initialization_warehouse
        update_model(
            project, "dynamic_table_init_wh", models.DYNAMIC_TABLE_WITH_INIT_WAREHOUSE_ALTER
        )
        run_dbt(["run"])

        # Verify change was applied
        dt_after = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_after.snowflake_initialization_warehouse == "DBT_TESTING"

    def test_unset_initialization_warehouse(self, project):
        """Verify initialization_warehouse can be unset (removed)."""
        # Initial state - has initialization_warehouse
        dt_before = describe_dynamic_table(project, "dynamic_table_init_wh")
        assert dt_before.snowflake_initialization_warehouse == "DBT_TESTING_ALT"

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
