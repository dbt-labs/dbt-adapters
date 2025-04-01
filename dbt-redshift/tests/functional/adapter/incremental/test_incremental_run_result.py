import pytest
from dbt.tests.util import run_dbt

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


class TestIncrementalRunResultRedshift(BaseIncremental):
    """Bonus test to verify that incremental models return the number of rows affected"""

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # run with initial seed
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # run with additions
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1
        # verify that run_result is correct
        rows_affected = results[0].adapter_response["rows_affected"]
        assert rows_affected == 10, f"Expected 10 rows changed, found {rows_affected}"


class TestIncrementalDeleteInsertRedshift(BaseIncremental):
    """Bonus test to verify that incremental models return the number of rows affected"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": files.incremental_delete_insert_sql,
            "schema.yml": files.schema_base_yml,
        }

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # run with initial seed
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # run with additions
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1
        # verify that run_result is correct
        rows_affected = results[0].adapter_response["rows_affected"]
        assert rows_affected == 10, f"Expected 10 rows changed, found {rows_affected}"
