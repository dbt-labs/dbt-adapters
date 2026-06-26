from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.catalog_tests import files


_INFORMATION_SCHEMA_SOURCE = """
sources:
  - name: information_schema_src
    schema: "region-us.INFORMATION_SCHEMA"
    tables:
      - name: column_field_paths
        identifier: COLUMN_FIELD_PATHS
"""


class TestDocsGenerateWithInformationSchemaSource:
    """Regression test for https://github.com/dbt-labs/dbt-adapters/issues/1005

    Defining a source that references INFORMATION_SCHEMA should not break
    `dbt docs generate`. The catalog query must skip schemas that are not
    real BigQuery datasets rather than producing a malformed project ID.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": files.MY_TABLE,
            "_sources.yml": _INFORMATION_SCHEMA_SOURCE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    def test_catalog_has_no_errors(self, docs: CatalogArtifact):
        assert not docs.errors

    def test_normal_nodes_in_catalog(self, docs: CatalogArtifact):
        assert "model.test.my_table" in docs.nodes

    def test_information_schema_source_excluded_from_catalog(self, docs: CatalogArtifact):
        assert "source.test.information_schema_src.column_field_paths" not in docs.sources
