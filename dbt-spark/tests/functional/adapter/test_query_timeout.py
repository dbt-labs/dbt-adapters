"""Functional tests for query timeout and retry behavior.

NOTE: These tests only work with PyHive-based connections (http/thrift methods).
ODBC connections use PyodbcConnectionWrapper which doesn't have timeout/retry support yet.
"""

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt_common.exceptions import DbtRuntimeError


# Model that creates a delay to simulate a long-running query
# Uses a large cross join to create computational delay
long_running_model = """
{{ config(materialized='table') }}
-- Generate a large dataset to create a delay
-- This creates roughly 1 million rows which should take several seconds
with numbers as (
  select explode(sequence(1, 100000)) as n
),
cross_product as (
  select a.n as n1, b.n as n2
  from numbers a
  cross join numbers b
)
select count(*) as total_count
from cross_product
"""

simple_model = """
{{ config(materialized='table') }}
select 1 as id
"""


@pytest.mark.skip_profile(
    "spark_http_odbc",
    "databricks_cluster",
    "databricks_sql_endpoint",
    "databricks_http_cluster",
    "spark_session",
)
class TestQueryTimeout:
    """Test query timeout functionality.

    Skipped on ODBC profiles because PyodbcConnectionWrapper doesn't use
    the async polling mechanism with timeout support.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "long_running.sql": long_running_model,
            "simple.sql": simple_model,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        """Override profile to add timeout configuration."""
        dbt_profile_target["query_timeout"] = 1  # 1 second timeout
        dbt_profile_target["poll_interval"] = 1  # Poll every second for faster test
        dbt_profile_target["query_retries"] = 0  # Disable retries for clearer errors
        return dbt_profile_target

    def test_query_timeout_exceeded(self, project):
        """Test that queries exceeding timeout raise appropriate error."""

        # Simple model should succeed (runs quickly)
        results = run_dbt(["run", "--select", "simple"])
        assert results[0].status == "success"

        # Long-running model should timeout
        # The cross-join query should take longer than 2 seconds on most systems
        _, output = run_dbt_and_capture(["run", "--select", "long_running"], expect_pass=False)

        assert "exceeded timeout" in output.lower()
