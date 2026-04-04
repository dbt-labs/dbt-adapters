import pytest
from unittest.mock import patch

from dbt.tests.adapter.unit_testing.test_pseudocolumns import (
    BasePseudocolumnUnitTest,
    external_table_csv,
)


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
    """Test BigQuery's _FILE_NAME pseudocolumn support in unit tests.

    Uses BigQuery-specific SQL (_FILE_NAME as file_name) and validates that the
    pseudocolumn framework works with BigQueryColumn types.
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
    def setup_pseudocolumn_override(self, project):
        """Patch BigQueryAdapter at the class level to return _FILE_NAME for external_table.

        Instance-level patching (project.adapter.X) doesn't work because dbtRunner
        creates its own adapter instance. Class-level patching affects all instances.
        """
        from dbt.adapters.bigquery.column import BigQueryColumn
        from dbt.adapters.bigquery.impl import BigQueryAdapter

        original_method = BigQueryAdapter.get_pseudocolumns_for_relation

        def mock_pseudocolumns(self_adapter, relation):
            if relation.identifier == "external_table":
                return [BigQueryColumn("_FILE_NAME", "STRING")]
            return original_method(self_adapter, relation)

        mock_pseudocolumns._is_available_ = True
        mock_pseudocolumns._parse_replacement_ = lambda *a, **k: []

        with patch.object(BigQueryAdapter, "get_pseudocolumns_for_relation", mock_pseudocolumns):
            yield
