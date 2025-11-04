import pytest
from dbt.tests.util import run_dbt


my_model_sql = """
select
    * from {{ ref('my_upstream_model')}}
"""

my_upstream_model_with_reserved_word_column_name_sql = """
select 1 as "GROUP"
"""

test_my_model_csv_fixtures_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        format: csv
        fixture: input_fixture
    expect:
      format: csv
      fixture: expect_fixture
"""

input_fixture_csv = """
GROUP
1
2
3
"""

expect_fixture_csv = """
GROUP
1
2
3
"""


class BaseUnitTestQuotedReservedWordColumnNames:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_upstream_model.sql": my_upstream_model_with_reserved_word_column_name_sql,
            "unit_tests.yml": test_my_model_csv_fixtures_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fixtures": {
                "input_fixture.csv": input_fixture_csv,
                "expect_fixture.csv": expect_fixture_csv,
            },
        }

    def test_quoted_reserved_word_column_names(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])
