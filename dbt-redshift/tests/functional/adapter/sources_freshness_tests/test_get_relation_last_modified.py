import os
import pytest
from unittest import mock

from dbt.adapters.redshift.impl import RedshiftAdapter
from dbt.adapters.capability import Capability, CapabilityDict
from dbt.cli.main import dbtRunner
from dbt.tests.util import run_dbt

from tests.functional.adapter.sources_freshness_tests import files


class SetupGetLastRelationModified:
    @pytest.fixture(scope="class", autouse=True)
    def set_env_vars(self, project):
        os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"] = project.test_schema
        yield
        del os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]


class TestGetLastRelationModified(SetupGetLastRelationModified):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_source_no_last_modified.csv": files.SEED_TEST_SOURCE_NO_LAST_MODIFIED_CSV,
            "test_source_last_modified.csv": files.SEED_TEST_SOURCE_LAST_MODIFIED_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.SCHEMA_YML}

    @pytest.mark.parametrize(
        "source,status,expect_pass",
        [
            ("test_source.test_source_no_last_modified", "pass", True),
            ("test_source.test_source_last_modified", "error", False),  # stale
        ],
    )
    def test_get_last_relation_modified(self, project, source, status, expect_pass):
        run_dbt(["seed"])

        results = run_dbt(
            ["source", "freshness", "--select", f"source:{source}"], expect_pass=expect_pass
        )
        assert len(results) == 1
        result = results[0]
        assert result.status == status


freshness_metadata_schema_batch_yml = """
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_table
      - name: test_table2
      - name: test_table_with_loaded_at_field
        loaded_at_field: my_loaded_at_field
"""


class TestGetLastRelationModifiedBatch(SetupGetLastRelationModified):
    @pytest.fixture(scope="class")
    def custom_schema(self, project, set_env_vars):
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]
            )
            project.adapter.drop_schema(relation)
            project.adapter.create_schema(relation)

        yield relation.schema

        with project.adapter.connection_named("__test"):
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_metadata_schema_batch_yml}

    def get_freshness_result_for_table(self, table_name, results):
        for result in results:
            if result.node.name == table_name:
                return result
        return None

    def test_get_last_relation_modified_batch(self, project, custom_schema):
        project.run_sql(
            f"create table {custom_schema}.test_table as (select 1 as id, 'test' as name);"
        )
        project.run_sql(
            f"create table {custom_schema}.test_table2 as (select 1 as id, 'test' as name);"
        )
        project.run_sql(
            f"create table {custom_schema}.test_table_with_loaded_at_field as (select 1 as id, timestamp '2009-09-15 10:59:43' as my_loaded_at_field);"
        )

        runner = dbtRunner()
        freshness_results_batch = runner.invoke(["source", "freshness"]).result

        assert len(freshness_results_batch) == 3
        test_table_batch_result = self.get_freshness_result_for_table(
            "test_table", freshness_results_batch
        )
        test_table2_batch_result = self.get_freshness_result_for_table(
            "test_table2", freshness_results_batch
        )
        test_table_with_loaded_at_field_batch_result = self.get_freshness_result_for_table(
            "test_table_with_loaded_at_field", freshness_results_batch
        )

        # Remove TableLastModifiedMetadataBatch and run freshness on same input without batch strategy
        capabilities_no_batch = CapabilityDict(
            {
                capability: support
                for capability, support in RedshiftAdapter.capabilities().items()
                if capability != Capability.TableLastModifiedMetadataBatch
            }
        )
        with mock.patch.object(
            RedshiftAdapter, "capabilities", return_value=capabilities_no_batch
        ):
            freshness_results = runner.invoke(["source", "freshness"]).result

        assert len(freshness_results) == 3
        test_table_result = self.get_freshness_result_for_table("test_table", freshness_results)
        test_table2_result = self.get_freshness_result_for_table("test_table2", freshness_results)
        test_table_with_loaded_at_field_result = self.get_freshness_result_for_table(
            "test_table_with_loaded_at_field", freshness_results
        )

        # assert results between batch vs non-batch freshness strategy are equivalent
        assert test_table_result.status == test_table_batch_result.status
        assert test_table_result.max_loaded_at == test_table_batch_result.max_loaded_at

        assert test_table2_result.status == test_table2_batch_result.status
        assert test_table2_result.max_loaded_at == test_table2_batch_result.max_loaded_at

        assert (
            test_table_with_loaded_at_field_batch_result.status
            == test_table_with_loaded_at_field_result.status
        )
        assert (
            test_table_with_loaded_at_field_batch_result.max_loaded_at
            == test_table_with_loaded_at_field_result.max_loaded_at
        )
