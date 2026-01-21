import pytest
from unittest import mock

from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.tests.util import run_dbt


_MODEL_SQL = """
{{ config(materialized='table') }}
select 1 as id
"""

_SCHEMA_YML = """
version: 2

models:
  - name: my_model
    description: My model description
"""


class TestNoopAlterRelationCommentBehaviorFlag:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _MODEL_SQL,
            "schema.yml": _SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"bigquery_noop_alter_relation_comment": True},
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                    }
                }
            },
        }

    def test_relation_description_set_without_update_call(self, project):
        with mock.patch.object(BigQueryAdapter, "update_table_description") as mock_update:
            run_dbt(["run"])
            assert mock_update.call_count == 0

        with project.adapter.connection_named("_test"):
            client = project.adapter.connections.get_thread_connection().handle
            table_id = f"{project.database}.{project.test_schema}.my_model"
            bq_table = client.get_table(table_id)

        assert bq_table.description
        assert bq_table.description.startswith("My model description")
