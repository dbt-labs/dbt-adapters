"""
Regression test for dbt-core#11067.

When a column has the same name as its table, BigQuery resolves an unqualified
column reference to the row STRUCT (containing all of the row's columns) rather
than to the column value. The default `unique` and `not_null` generic tests
emit unqualified column references, which means the resulting SQL filters and
groups against the row struct instead of the column. After the fix in
`bigquery__test_unique` and `bigquery__test_not_null`, the source relation is
aliased and column references are qualified, so the tests evaluate against the
column as expected.
"""

import pytest

from dbt.tests.util import run_dbt


_MODEL_SQL = """
select 1 as orders union all
select 1 as orders union all
select 2 as orders union all
select cast(null as int64) as orders
""".lstrip()


_SCHEMA_YML = """
version: 2
models:
  - name: orders
    columns:
      - name: orders
        data_tests:
          - unique
          - not_null
""".lstrip()


class TestBigQueryGenericTestsColumnTableNameCollision:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": _MODEL_SQL,
            "schema.yml": _SCHEMA_YML,
        }

    def test_unique_and_not_null_detect_failures(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)

        assert len(results) == 2

        statuses = {r.node.name: (r.status, r.failures) for r in results}

        unique_status, unique_failures = statuses["unique_orders_orders"]
        assert unique_status == "fail", (
            f"unique test passed unexpectedly (status={unique_status}); "
            "with the bug, the row-struct grouping can mask duplicates"
        )
        assert unique_failures >= 1

        not_null_status, not_null_failures = statuses["not_null_orders_orders"]
        assert not_null_status == "fail", (
            f"not_null test passed unexpectedly (status={not_null_status}); "
            "with the bug, the row-struct nullness check masks the null column value"
        )
        assert not_null_failures >= 1
