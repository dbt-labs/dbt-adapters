import pytest

from dbt.adapters.contracts.relation import Column
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
external_table_csv = """id
1
2
3
"""


class BasePseudocolumnUnitTest:
    """Base test class for pseudocolumn support in unit tests.

    Pseudocolumns are system-generated columns that can be queried but don't
    appear in the information schema (e.g., BigQuery's _FILE_NAME for external tables).

    This test validates that unit tests can reference columns that don't exist in the
    actual table by using the get_pseudocolumns_for_relation() adapter method. The test
    injects a mock pseudocolumn via pytest fixture to simulate real pseudocolumn behavior
    without requiring adapter-specific features.

    Key validation points:
    - The seed file does NOT contain the pseudocolumn
    - The adapter's get_pseudocolumns_for_relation() is overridden to return it
    - Unit tests can use the pseudocolumn in fixtures without validation errors
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

    @pytest.fixture(scope="class")
    def setup_pseudocolumn_override(self, project):
        """Override adapter method to inject a pseudocolumn that doesn't exist in the seed.

        This simulates real pseudocolumn behavior where columns are queryable but
        don't appear in the information schema.
        """
        original_method = project.adapter.get_pseudocolumns_for_relation

        def mock_get_pseudocolumns(relation):
            # Return pseudocolumn only for our test source table
            if relation.identifier == 'external_table':
                return [Column('pseudocolumn_value', 'text')]
            return []

        project.adapter.get_pseudocolumns_for_relation = mock_get_pseudocolumns
        yield
        # Restore original method after test
        project.adapter.get_pseudocolumns_for_relation = original_method

    def test_pseudocolumn_in_unit_test(self, setup_pseudocolumn_override):
        """Test that pseudocolumns can be included in unit test fixtures.

        This verifies that:
        1. Unit tests don't raise "Invalid column name" errors for pseudocolumns
        2. Pseudocolumn values can be provided in fixture data
        3. The unit test passes with pseudocolumn data

        The fixture injects 'pseudocolumn_value' as a pseudocolumn that doesn't
        exist in the seed file, simulating real pseudocolumn behavior.
        """
        # Seed the source table
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the unit test
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1


class TestPostgresPseudocolumnUnitTest(BasePseudocolumnUnitTest):
    """Postgres test using patched pseudocolumn support.

    This test validates the pseudocolumn feature by:
    1. Creating a seed with only 'id' column (no pseudocolumn_value)
    2. Injecting 'pseudocolumn_value' as a pseudocolumn via fixture
    3. Verifying unit tests can use the pseudocolumn even though it doesn't
       exist in the actual table

    This demonstrates that the feature works correctly for columns that
    are queryable but don't appear in information_schema.
    """

    pass
