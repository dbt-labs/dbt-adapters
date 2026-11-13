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


# Same column/table name collision but with a `where:` filter on each test.
# `get_where_subquery` renders `model` as `(select * from <rel> where ...)
# dbt_subquery`; the BigQuery overrides must still compile and execute against
# that already-aliased subquery.
_SCHEMA_WHERE_YML = """
version: 2
models:
  - name: orders
    columns:
      - name: orders
        data_tests:
          - unique:
              config:
                where: "orders is not null"
          - not_null:
              config:
                where: "orders is not null or orders is null"
""".lstrip()


def _result_for_prefix(results, prefix):
    """Look up a test result by node-name prefix.

    Generic test node names (`unique_<model>_<column>` etc.) are stable today
    but tying assertions to the literal name makes the test brittle across dbt
    versions if naming gains suffixes. Match on prefix and surface a clear
    error if the expected result is missing.
    """
    matches = [r for r in results if r.node.name.startswith(prefix)]
    assert matches, (
        f"expected a test result whose node name starts with {prefix!r}; "
        f"found: {sorted(r.node.name for r in results)}"
    )
    assert len(matches) == 1, (
        f"expected exactly one result for prefix {prefix!r}; "
        f"got: {[r.node.name for r in matches]}"
    )
    return matches[0]


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

        unique_result = _result_for_prefix(results, "unique_")
        assert unique_result.status == "fail", (
            f"unique test passed unexpectedly (status={unique_result.status}); "
            "with the bug, the row-struct grouping can mask duplicates"
        )
        assert unique_result.failures >= 1

        not_null_result = _result_for_prefix(results, "not_null_")
        assert not_null_result.status == "fail", (
            f"not_null test passed unexpectedly (status={not_null_result.status}); "
            "with the bug, the row-struct nullness check masks the null column value"
        )
        assert not_null_result.failures >= 1


class TestBigQueryGenericTestsColumnTableNameCollisionWithWhere:
    """`where:`-filtered tests render `model` as `(select * from <rel> where ...)
    dbt_subquery`. The BigQuery overrides wrap `model` in another subquery
    before aliasing so this case still produces valid SQL."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": _MODEL_SQL,
            "schema.yml": _SCHEMA_WHERE_YML,
        }

    def test_unique_and_not_null_compile_with_where_filter(self, project):
        run_dbt(["run"])
        # `expect_pass=False` because the `not_null` test still observes the
        # null row (its `where:` keeps null values); the `unique` test still
        # sees duplicates of `orders=1`. The important assertion is that the
        # SQL compiles and runs at all when `model` is an aliased subquery.
        results = run_dbt(["test"], expect_pass=False)

        assert len(results) == 2

        unique_result = _result_for_prefix(results, "unique_")
        assert unique_result.status == "fail", (
            f"unique test status was {unique_result.status}; "
            "expected fail because duplicate non-null orders remain after the where filter"
        )
        assert unique_result.failures >= 1

        not_null_result = _result_for_prefix(results, "not_null_")
        assert not_null_result.status == "fail", (
            f"not_null test status was {not_null_result.status}; "
            "expected fail because the where filter retains the null row"
        )
        assert not_null_result.failures >= 1
