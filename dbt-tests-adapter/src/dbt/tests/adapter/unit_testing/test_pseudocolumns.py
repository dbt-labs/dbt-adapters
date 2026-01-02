import pytest

from dbt.tests.util import run_dbt


# Model that uses a pseudocolumn from a source
my_model_sql = """
select
    id,
    pseudocolumn_value
from {{ source('test_source', 'external_table') }}
"""

# Source definition for external table
schema_sources_yml = """
sources:
  - name: test_source
    schema: "{{ target.schema }}"
    tables:
      - name: external_table
        columns:
          - name: id
"""

# Unit test that includes a pseudocolumn in the fixture
test_my_model_yml = """
unit_tests:
  - name: test_pseudocolumn_support
    model: my_model
    given:
      - input: source('test_source', 'external_table')
        rows:
          - {id: 1, pseudocolumn_value: 'test_value'}
    expect:
      rows:
        - {id: 1, pseudocolumn_value: 'test_value'}
"""

# Seed file to create the source table
external_table_csv = """id,pseudocolumn_value
1,seed_value_1
2,seed_value_2
3,seed_value_3
"""


class BasePseudocolumnUnitTest:
    """Base test class for pseudocolumn support in unit tests.

    Pseudocolumns are system-generated columns that can be queried but don't
    appear in the information schema (e.g., BigQuery's _FILE_NAME for external tables).

    This test uses a source to represent an external table or similar construct
    that would have pseudocolumns in a real database environment.

    This base test uses a generic 'pseudocolumn_value' placeholder so it can
    run on all adapters. Adapters with actual pseudocolumn support should create
    their own concrete tests using real pseudocolumn names.

    See dbt-bigquery/tests/functional/adapter/unit_testing/test_pseudocolumns.py
    for an example implementation using BigQuery's _FILE_NAME pseudocolumn.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        """Seed data to create the source table for testing."""
        return {
            "external_table.csv": external_table_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Models and source definitions for the test."""
        return {
            "my_model.sql": my_model_sql,
            "sources.yml": schema_sources_yml + test_my_model_yml,
        }

    def test_pseudocolumn_in_unit_test(self):
        """Test that pseudocolumns can be included in unit test fixtures.

        This verifies that:
        1. Unit tests don't raise "Invalid column name" errors for pseudocolumns
        2. Pseudocolumn values can be provided in fixture data
        3. The unit test passes with pseudocolumn data
        """
        # Seed the source table
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the model that uses the source with pseudocolumn
        results = run_dbt(["run"])
        assert len(results) == 1

        # Run the unit test - should pass without validation errors
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1


class TestPostgresPseudocolumnUnitTest(BasePseudocolumnUnitTest):
    """Postgres doesn't currently have pseudocolumn support configured.

    This test will pass using the base implementation which returns
    an empty pseudocolumn list. The 'pseudocolumn_value' is treated as
    a regular column from the seed data.
    """

    pass
