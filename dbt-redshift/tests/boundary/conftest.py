from datetime import datetime, timezone
import os
import random

import pytest
import redshift_connector


@pytest.fixture
def connection() -> redshift_connector.Connection:
    return redshift_connector.connect(
        user=os.getenv("REDSHIFT_TEST_USER"),
        password=os.getenv("REDSHIFT_TEST_PASS"),
        host=os.getenv("REDSHIFT_TEST_HOST"),
        port=int(os.getenv("REDSHIFT_TEST_PORT", 5439)),
        database=os.getenv("REDSHIFT_TEST_DBNAME"),
        region=os.getenv("REDSHIFT_TEST_REGION"),
    )


@pytest.fixture
def schema_name(request) -> str:
    runtime = datetime.now(timezone.utc).replace(tzinfo=None) - datetime(1970, 1, 1, 0, 0, 0)
    runtime_s = int(runtime.total_seconds())
    runtime_ms = runtime.microseconds
    random_int = random.randint(0, 9999)
    file_name = request.module.__name__.split(".")[-1]
    return f"test_{runtime_s}{runtime_ms}{random_int:04}_{file_name}"
