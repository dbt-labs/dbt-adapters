import pytest
from dbt.tests.util import run_dbt


my_model_sql = """
select
    tested_column from {{ ref('my_upstream_model')}}
"""

my_upstream_model_sql = """
select 1 as tested_column
"""

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {tested_column: 1}
          - {TESTED_COLUMN: 2}
          - {tested_colUmn: 3}
    expect:
      rows:
          - {tested_column: 1}
          - {TESTED_COLUMN: 2}
          - {tested_colUmn: 3}
"""


class BaseUnitTestCaseInsensivity:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_upstream_model.sql": my_upstream_model_sql,
            "unit_tests.yml": test_my_model_yml,
        }

    def test_case_insensitivity(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])


class TestPosgresUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass
