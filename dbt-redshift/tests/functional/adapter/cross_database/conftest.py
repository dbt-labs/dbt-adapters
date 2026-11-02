import pytest

from tests.functional.adapter.cross_database.fixtures import REDSHIFT_TEST_CROSS_DBNAME


@pytest.fixture(autouse=True, scope="session")
def _skip_without_cross_db():
    """Skip every test in this directory when the env var is not set."""
    if not REDSHIFT_TEST_CROSS_DBNAME:
        pytest.skip("REDSHIFT_TEST_CROSS_DBNAME not set")
