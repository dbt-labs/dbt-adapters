import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.util import check_result_nodes_by_name, run_dbt


class BaseSingularTestsEphemeral:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": files.seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": files.ephemeral_with_cte_sql,
            "passing_model.sql": files.test_ephemeral_passing_sql,
            "failing_model.sql": files.test_ephemeral_failing_sql,
            "schema.yml": files.schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "passing.sql": files.test_ephemeral_passing_sql,
            "failing.sql": files.test_ephemeral_failing_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "singular_tests_ephemeral",
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_singular_tests_ephemeral(self, project):
        # check results from seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # Check results from test command
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 2
        check_result_nodes_by_name(results, ["passing", "failing"])

        # Check result status
        for result in results:
            if result.node.name == "passing":
                assert result.status == "pass"
            elif result.node.name == "failing":
                assert result.status == "fail"

        # check results from run command
        results = run_dbt()
        assert len(results) == 2
        check_result_nodes_by_name(results, ["failing_model", "passing_model"])


class TestSingularTestsEphemeral(BaseSingularTestsEphemeral):
    pass
