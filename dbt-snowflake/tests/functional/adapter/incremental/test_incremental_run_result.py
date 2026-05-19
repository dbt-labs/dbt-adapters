from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


class TestIncrementalRunResultSnowflake(BaseIncremental):
    """Test to verify that incremental models return adapter response stats including
    DML statistics (rows_inserted, rows_updated, rows_deleted, rows_duplicates).

    This test checks both:
    1. Initial CTAS operation when the incremental table doesn't exist yet
    2. Incremental merge operation when the table already exists
    """

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # First run with initial seed - this is a CTAS operation since the incremental table doesn't exist
        results, output = run_dbt_and_capture(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # Verify CTAS adapter response includes DML stats
        adapter_response = results[0].adapter_response
        assert "code" in adapter_response
        assert adapter_response["code"] == "SUCCESS"

        # For CTAS, rows_inserted should equal the number of rows created (10 from base seed)
        assert "rows_inserted" in adapter_response
        assert (
            adapter_response["rows_inserted"] == 10
        ), f"Expected 10 rows inserted for CTAS, found {adapter_response['rows_inserted']}"

        # Other DML stats should be 0 for CTAS
        assert "rows_deleted" in adapter_response
        assert adapter_response["rows_deleted"] == 0
        assert "rows_updated" in adapter_response
        assert adapter_response["rows_updated"] == 0
        assert "rows_duplicates" in adapter_response
        assert adapter_response["rows_duplicates"] == 0

        # Verify stdout shows the row count for CTAS
        assert (
            "SUCCESS 10" in output
        ), f"Expected 'SUCCESS 10' in stdout for CTAS, but got: {output}"

        # Second run with additions - this is an incremental merge operation
        results, output = run_dbt_and_capture(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # Verify incremental merge adapter response
        adapter_response = results[0].adapter_response
        rows_affected = adapter_response["rows_affected"]
        assert rows_affected == 10, f"Expected 10 rows changed, found {rows_affected}"

        # For incremental merge, rows_inserted should reflect new rows added
        assert "rows_inserted" in adapter_response
        assert (
            adapter_response["rows_inserted"] == 10
        ), f"Expected 10 rows inserted for merge, found {adapter_response['rows_inserted']}"

        # Verify stdout shows the row count for incremental merge
        assert (
            "SUCCESS 10" in output
        ), f"Expected 'SUCCESS 10' in stdout for incremental merge, but got: {output}"
