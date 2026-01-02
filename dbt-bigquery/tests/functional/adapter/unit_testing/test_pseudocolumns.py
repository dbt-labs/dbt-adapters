import pytest

from dbt.tests.adapter.unit_testing.test_pseudocolumns import (
    BasePseudocolumnUnitTest,
    external_table_csv,
)
from dbt.tests.util import run_dbt


# Model that uses BigQuery's _FILE_NAME pseudocolumn
my_model_sql = """
select
    id,
    _FILE_NAME as file_name
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

# Unit test that includes _FILE_NAME pseudocolumn in the fixture
test_my_model_yml = """
unit_tests:
  - name: test_bigquery_file_name_pseudocolumn
    model: my_model
    given:
      - input: source('test_source', 'external_table')
        rows:
          - {id: 1, _FILE_NAME: 'gs://bucket/file1.csv'}
          - {id: 2, _FILE_NAME: 'gs://bucket/file2.csv'}
    expect:
      rows:
        - {id: 1, file_name: 'gs://bucket/file1.csv'}
        - {id: 2, file_name: 'gs://bucket/file2.csv'}
"""


class TestBigQueryPseudocolumns(BasePseudocolumnUnitTest):
    """Test BigQuery's _FILE_NAME pseudocolumn support for external tables.

    This test validates that BigQuery's get_pseudocolumns_for_relation() correctly
    returns _FILE_NAME for external tables, allowing unit tests to use this
    pseudocolumn even though it doesn't exist in the seed data.

    The test mocks the relation type to appear as External, which triggers
    BigQuery's real pseudocolumn logic without requiring actual GCS external tables.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        """Seed data."""
        return {
            "external_table.csv": external_table_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Models using BigQuery's _FILE_NAME pseudocolumn."""
        return {
            "my_model.sql": my_model_sql,
            "sources.yml": schema_sources_yml + test_my_model_yml,
        }

    @pytest.fixture(scope="class")
    def mock_external_table(self, project):
        """Mock BigQuery table metadata to appear as an External table.

        This allows BigQuery's actual get_pseudocolumns_for_relation() to return
        _FILE_NAME without requiring a real external table in GCS.
        """
        original_get_bq_table = project.adapter.connections.get_bq_table

        def mock_get_bq_table(database, schema, identifier):
            # Get the real table object
            table = original_get_bq_table(database, schema, identifier)

            # If it's our test table, override the table_type to be EXTERNAL
            if identifier == 'external_table':
                # Create a mock-like object that preserves the original table
                # but returns "EXTERNAL" for table_type
                class MockTable:
                    def __init__(self, original_table):
                        self._original = original_table

                    def __getattr__(self, name):
                        if name == 'table_type':
                            return 'EXTERNAL'
                        return getattr(self._original, name)

                    @property
                    def table_type(self):
                        return 'EXTERNAL'

                return MockTable(table)

            return table

        project.adapter.connections.get_bq_table = mock_get_bq_table
        yield
        project.adapter.connections.get_bq_table = original_get_bq_table

    def test_pseudocolumn_in_unit_test(self, mock_external_table):
        """Test that _FILE_NAME pseudocolumn works in unit tests.

        This verifies that:
        1. The seed doesn't contain _FILE_NAME column
        2. BigQuery's get_pseudocolumns_for_relation() returns it for external tables
        3. Unit tests can use _FILE_NAME in fixtures without validation errors
        4. The unit test passes with _FILE_NAME data
        """
        # Seed the source table (without _FILE_NAME column)
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the unit test - should pass because pseudocolumn is available for unit tests
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
