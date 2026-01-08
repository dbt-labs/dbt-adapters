import os
import pytest
from unittest import mock

from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.capability import Capability, CapabilityDict
from dbt.tests.util import run_dbt
from dbt.cli.main import dbtRunner

from tests.functional.adapter.sources_freshness_tests import files


class TestGetLastRelationModified:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"test_source.csv": files.SEED_TEST_SOURCE_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.SCHEMA_YML}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        # we need the schema name for the sources section
        os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"] = project.test_schema
        run_dbt(["seed"])
        yield
        del os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]

    def test_get_last_relation_modified(self, project):
        results = run_dbt(["source", "freshness"])
        assert len(results) == 1
        result = results[0]
        assert result.status == "pass"


class TestGetLastRelationModifiedBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.BATCH_SCHEMA_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "bigquery_use_batch_source_freshness": True,
            }
        }

    def get_freshness_result_for_table(self, table_name, results):
        for result in results:
            if result.node.name == table_name:
                return result
        return None

    def test_get_last_relation_modified_batch(self, project, unique_schema, logs_dir):
        project.run_sql(
            f"create table {unique_schema}.test_table as (select 1 as id, 'test' as name);"
        )
        project.run_sql(
            f"create table {unique_schema}.test_table2 as (select 1 as id, 'test' as name);"
        )
        project.run_sql(
            f"create table {unique_schema}.test_table_with_loaded_at_field as (select 1 as id, timestamp '2009-09-15 10:59:43' as my_loaded_at_field);"
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

        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            assert "INFORMATION_SCHEMA.TABLE_STORAGE" in log

        # Remove TableLastModifiedMetadataBatch and run freshness on same input without batch strategy
        capabilities_no_batch = CapabilityDict(
            {
                capability: support
                for capability, support in BigQueryAdapter.capabilities().items()
                if capability != Capability.TableLastModifiedMetadataBatch
            }
        )
        with mock.patch.object(
            BigQueryAdapter, "capabilities", return_value=capabilities_no_batch
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


class TestGetLastRelationModifiedBatchLegacy:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.BATCH_SCHEMA_YML}

    def test_get_last_relation_modified_batch(self, project, unique_schema, logs_dir):
        project.run_sql(
            f"create table {unique_schema}.test_table as (select 1 as id, 'test' as name);"
        )
        project.run_sql(
            f"create table {unique_schema}.test_table2 as (select 1 as id, 'test' as name);"
        )
        project.run_sql(
            f"create table {unique_schema}.test_table_with_loaded_at_field as (select 1 as id, timestamp '2009-09-15 10:59:43' as my_loaded_at_field);"
        )

        runner = dbtRunner()
        runner.invoke(["source", "freshness"])

        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            assert "INFORMATION_SCHEMA.TABLE_STORAGE" not in log
