import os

from dbt.tests.util import get_connection
import pytest


REDSHIFT_TEST_CROSS_DBNAME = os.getenv("REDSHIFT_TEST_CROSS_DBNAME", "")

skip_if_no_cross_db = pytest.mark.skipif(
    not REDSHIFT_TEST_CROSS_DBNAME,
    reason="REDSHIFT_TEST_CROSS_DBNAME not set — skipping cross-database tests",
)


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
