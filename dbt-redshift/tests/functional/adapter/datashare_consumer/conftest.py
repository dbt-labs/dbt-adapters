import pytest

from tests.functional.adapter.datashare_consumer.fixtures import (
    REDSHIFT_TEST_DATASHARE_DBNAME,
)


@pytest.fixture(autouse=True, scope="session")
def _skip_without_datashare_db():
    """Skip every test in this directory when the env var is not set."""
    if not REDSHIFT_TEST_DATASHARE_DBNAME:
        pytest.skip("REDSHIFT_TEST_DATASHARE_DBNAME not set")
