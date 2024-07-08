import pytest
import json

from dbt.tests.adapter.materialized_view import files
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from tests.functional.utils import run_dbt

_MATERIALIZED_VIEW_PROPERTIES__SCHEMA_YML = """
version: 2

models:
  - name: my_materialized_view
    description: |
      Materialized view model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
"""


class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass


class TestPersistDocsWithMaterializedView(BasePersistDocs):
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_materialized_view.sql": files.MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "schema.yml": _MATERIALIZED_VIEW_PROPERTIES__SCHEMA_YML,
        }

    def test_has_comments_pglike(self, project):
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 2
        view_node = catalog_data["nodes"]["model.test.my_materialized_view"]
        assert view_node["metadata"]["comment"].startswith("Materialized view model description")
