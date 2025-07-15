import pytest

from dbt.tests.adapter.store_test_failures_tests import fixtures
from dbt.tests.util import check_relations_equal, run_dbt


# used to rename test audit schema to help test schema meet max char limit
# the default is _dbt_test__audit but this runs over the postgres 63 schema name char limit
# without which idempotency conditions will not hold (i.e. dbt can't drop the schema properly)
TEST_AUDIT_SCHEMA_SUFFIX = "dbt_test__aud"


class StoreTestFailuresBase:
    @pytest.fixture(scope="function", autouse=True)
    def setUp(self, project):
        self.test_audit_schema = f"{project.test_schema}_{TEST_AUDIT_SCHEMA_SUFFIX}"
        run_dbt(["seed"])
        run_dbt(["run"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": fixtures.seeds__people,
            "expected_accepted_values.csv": fixtures.seeds__expected_accepted_values,
            "expected_failing_test.csv": fixtures.seeds__expected_failing_test,
            "expected_not_null_problematic_model_id.csv": fixtures.seeds__expected_not_null_problematic_model_id,
            "expected_unique_problematic_model_id.csv": fixtures.seeds__expected_unique_problematic_model_id,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing_test.sql": fixtures.tests__failing_test,
            "passing_test.sql": fixtures.tests__passing_test,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": fixtures.properties__schema_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fine_model.sql": fixtures.models__fine_model,
            "fine_model_but_with_a_no_good_very_long_name.sql": fixtures.models__file_model_but_with_a_no_good_very_long_name,
            "problematic_model.sql": fixtures.models__problematic_model,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "test": self.column_type_overrides(),
            },
            "data_tests": {"+schema": TEST_AUDIT_SCHEMA_SUFFIX},
        }

    def column_type_overrides(self):
        return {}

    def run_tests_store_one_failure(self, project):
        run_dbt(["test"], expect_pass=False)

        # one test is configured with store_failures: true, make sure it worked
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.unique_problematic_model_id",
                "expected_unique_problematic_model_id",
            ],
        )

    def run_tests_store_failures_and_assert(self, project):
        # make sure this works idempotently for all tests
        run_dbt(["test", "--store-failures"], expect_pass=False)
        results = run_dbt(["test", "--store-failures"], expect_pass=False)

        # compare test results
        actual = [(r.status, r.failures) for r in results]
        expected = [
            ("pass", 0),
            ("pass", 0),
            ("pass", 0),
            ("pass", 0),
            ("fail", 2),
            ("fail", 2),
            ("fail", 2),
            ("fail", 10),
        ]
        assert sorted(actual) == sorted(expected)

        # compare test results stored in database
        check_relations_equal(
            project.adapter, [f"{self.test_audit_schema}.failing_test", "expected_failing_test"]
        )
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.not_null_problematic_model_id",
                "expected_not_null_problematic_model_id",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.unique_problematic_model_id",
                "expected_unique_problematic_model_id",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.accepted_values_problemat"
                "ic_mo_c533ab4ca65c1a9dbf14f79ded49b628",
                "expected_accepted_values",
            ],
        )


class BaseStoreTestFailures(StoreTestFailuresBase):
    @pytest.fixture(scope="function")
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=self.test_audit_schema
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def column_type_overrides(self):
        return {
            "expected_unique_problematic_model_id": {
                "+column_types": {
                    "n_records": "bigint",
                },
            },
            "expected_accepted_values": {
                "+column_types": {
                    "n_records": "bigint",
                },
            },
        }

    def test__store_and_assert(self, project, clean_up):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)


class BaseStoreTestFailuresLimit(BaseStoreTestFailures):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": fixtures.seeds__people,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    @pytest.fixture(scope="class")
    def tests(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"_seeds.yml": fixtures.properties__seeds_yml}

    def test__store_and_assert(self, project, clean_up):
        pass

    def test_store_limit(self, project, clean_up):
        results = run_dbt(["test"], expect_pass=False)
        # there are 9 actual failing rows, but the test `limit` config has a value of 4
        assert results.results[0].failures == 4
        relation_name = results.results[0].node.relation_name
        sql_result = project.run_sql(f"select count(*) as cnt from {relation_name}", fetch="one")
        count = sql_result[0] if sql_result is not None else None
        # make sure the table also only has 4 rows (not 9!)
        assert count == 4, f"The test failure count {count} doesn't match the config `limit` of 4"


class TestStoreTestFailures(BaseStoreTestFailures):
    pass


class TestStoreTestFailuresLimit(BaseStoreTestFailuresLimit):
    pass
