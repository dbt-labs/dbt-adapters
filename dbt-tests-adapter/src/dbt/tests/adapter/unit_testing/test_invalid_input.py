import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture


my_model_sql = """
select
    tested_column from {{ ref('my_upstream_model')}}
"""

my_upstream_model_sql = """
select 1 as tested_column
"""

test_my_model_yml = """
unit_tests:
  - name: test_invalid_input_column_name
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {invalid_column_name: 1}
    expect:
      rows:
          - {tested_column: 1}
  - name: test_invalid_expect_column_name
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {tested_column: 1}
    expect:
      rows:
          - {invalid_column_name: 1}
"""


class BaseUnitTestInvalidInput:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_upstream_model.sql": my_upstream_model_sql,
            "unit_tests.yml": test_my_model_yml,
        }

    def test_invalid_input(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_invalid_input_column_name"], expect_pass=False
        )
        assert (
            "Invalid column name: 'invalid_column_name' in unit test fixture for 'my_upstream_model'."
            in out
        )

        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_invalid_expect_column_name"], expect_pass=False
        )
        assert (
            "Invalid column name: 'invalid_column_name' in unit test fixture for expected output."
            in out
        )


class TestPostgresUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
