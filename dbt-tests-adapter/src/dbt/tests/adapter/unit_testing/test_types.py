import pytest

from dbt.tests.util import write_file, run_dbt

my_model_sql = """
select
    tested_column from {{ ref('my_upstream_model')}}
"""

my_upstream_model_sql = """
select
  {sql_value} as tested_column
"""

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {{ tested_column: {yaml_value} }}
    expect:
      rows:
        - {{ tested_column: {yaml_value} }}
"""


class BaseUnitTestingTypes:
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["TIMESTAMPTZ '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'1'::numeric", "1"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                """'{"bar": "baz", "balance": 7.77, "active": false}'""",
            ],
            # TODO: support complex types
            # ["ARRAY['a','b','c']", """'{"a", "b", "c"}'"""],
            # ["ARRAY[1,2,3]", """'{1, 2, 3}'"""],
        ]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_upstream_model.sql": my_upstream_model_sql,
            "schema.yml": test_my_model_yml,
        }

    def test_unit_test_data_type(self, project, data_types):
        for sql_value, yaml_value in data_types:
            # Write parametrized type value to sql files
            write_file(
                my_upstream_model_sql.format(sql_value=sql_value),
                "models",
                "my_upstream_model.sql",
            )

            # Write parametrized type value to unit test yaml definition
            write_file(
                test_my_model_yml.format(yaml_value=yaml_value),
                "models",
                "schema.yml",
            )

            results = run_dbt(["run", "--select", "my_upstream_model"])
            assert len(results) == 1

            try:
                run_dbt(["test", "--select", "my_model"])
            except Exception:
                raise AssertionError(f"unit test failed when testing model with {sql_value}")


_length_model_sql = """
select tested_column, length(tested_column) as col_len from {{ ref('my_upstream_model')}}
"""

_length_test_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {tested_column: "longer_string_value"}
    expect:
      rows:
        - {tested_column: "longer_string_value", col_len: 19}
"""


class BaseUnitTestingVarcharFixtureNoTruncation:
    """Regression test for https://github.com/dbt-labs/dbt-core/issues/11974

    Verifies that unit test fixture string values are not silently truncated
    when the upstream model's column has a narrow varchar type.

    Uses length() to detect truncation: if the fixture value is truncated from
    19 chars to 5, the model outputs col_len=5 which mismatches expected col_len=19.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _length_model_sql,
            "my_upstream_model.sql": my_upstream_model_sql.format(
                sql_value="cast('short' as varchar(5))"
            ),
            "schema.yml": _length_test_yml,
        }

    def test_varchar_fixture_not_truncated(self, project):
        results = run_dbt(["run", "--select", "my_upstream_model"])
        assert len(results) == 1

        run_dbt(["test", "--select", "my_model"])


class TestPostgresUnitTestingTypes(BaseUnitTestingTypes):
    pass
