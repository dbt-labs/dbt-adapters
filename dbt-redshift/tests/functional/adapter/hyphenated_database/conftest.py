import pytest

from tests.functional.adapter.hyphenated_database.fixtures import REDSHIFT_TEST_DBNAME_W_HYPHEN


@pytest.fixture(autouse=True, scope="session")
def _skip_without_hyphenated_db():
    """Skip every test in this directory when the env var is not set."""
    if not REDSHIFT_TEST_DBNAME_W_HYPHEN:
        pytest.skip("REDSHIFT_TEST_DBNAME_W_HYPHEN not set")
