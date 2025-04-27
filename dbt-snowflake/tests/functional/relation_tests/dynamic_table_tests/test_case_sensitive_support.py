import pytest

from dbt.tests.util import run_dbt

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import update_model


class TestCaseSensitiveSupport:
    """
    This test addresses https://github.com/dbt-labs/dbt-adapters/issues/917.

    Setting QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE overrides the quoting behavior in the metadata queries.
    This query:
        select
            "name",
            "schema_name",
            "database_name",
            "text",
            "target_lag",
            "warehouse",
            "refresh_mode"
        from table(result_scan(last_query_id()))
    returns these field names:
        NAME, SCHEMA_NAME, DATABASE_NAME, TEXT, TARGET_LAG, WAREHOUSE, REFRESH_MODE
    when QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE

    Relevant resources:
    https://docs.snowflake.com/en/sql-reference/parameters#quoted-identifiers-ignore-case
    https://docs.snowflake.com/en/sql-reference/identifiers-syntax#controlling-case-using-the-quoted-identifiers-ignore-case-parameter
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_dynamic_table.sql": models.DYNAMIC_TABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "on_configuration_change": "apply",
                "pre-hook": "alter session set quoted_identifiers_ignore_case = true",
                "post-hook": "alter session unset quoted_identifiers_ignore_case",
            }
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")
        project.run_sql(f"alter session unset quoted_identifiers_ignore_case")

    def test_changes_are_applied(self, project):

        # run dbt once and update the table so that the second run needs to query the metadata
        run_dbt(["run"])
        update_model(project, "my_dynamic_table", models.DYNAMIC_TABLE_ALTER)

        # run it a second time to trigger the metadata query
        run_dbt(["run"])

        # a successful run is a pass
        assert True
