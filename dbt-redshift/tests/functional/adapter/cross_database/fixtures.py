import os

from dbt.tests.util import get_connection
import pytest


REDSHIFT_TEST_CROSS_DBNAME = os.getenv("REDSHIFT_TEST_CROSS_DBNAME", "")


def assert_cross_db_relation_exists(adapter, schema, identifier):
    """Verify a relation exists in the cross-database."""
    with get_connection(adapter):
        relation = adapter.get_relation(REDSHIFT_TEST_CROSS_DBNAME, schema, identifier)
    assert (
        relation is not None
    ), f"Expected relation {identifier} in {REDSHIFT_TEST_CROSS_DBNAME}.{schema}"


class CrossDatabaseMixin:
    """Shared fixtures for cross-database tests.

    Sets +database at the project level to target all models to the
    cross-database, and enables datasharing to activate the SHOW APIs path.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+database": REDSHIFT_TEST_CROSS_DBNAME,
            }
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["datasharing"] = True
        return outputs

    @pytest.fixture(scope="class", autouse=True)
    def _cleanup_cross_db_schema(self, project_setup):
        """Drop the test schema in the cross-database after the test class.

        The framework's teardown only drops the test schema in the default
        (profile) database. Because these tests target models at
        REDSHIFT_TEST_CROSS_DBNAME via ``+database``, dbt creates the schema
        there too, and it would otherwise leak. This runs before the
        framework teardown (it depends on ``project_setup``).
        """
        yield
        adapter = project_setup.adapter
        with get_connection(adapter):
            relation = adapter.Relation.create(
                database=REDSHIFT_TEST_CROSS_DBNAME,
                schema=project_setup.test_schema,
            )
            adapter.drop_schema(relation)
