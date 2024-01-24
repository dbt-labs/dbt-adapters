import os

import pytest

from tests.functional.projects import dbt_integration


@pytest.fixture(scope="class")
def dbt_integration_project():
    return dbt_integration()


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "postgres",
        "host": os.getenv("POSTGRES_TEST_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
        "user": os.getenv("POSTGRES_TEST_USER", "root"),
        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
        "threads": int(os.getenv("POSTGRES_TEST_THREADS", 4)),
    }
