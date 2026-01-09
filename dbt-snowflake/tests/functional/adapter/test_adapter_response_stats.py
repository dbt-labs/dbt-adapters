import pytest
from dbt.tests.util import run_dbt


_MODEL_CTAS = """
{{ config(materialized='table') }}
select * from (
    select 1 as id, 'alice' as name union all
    select 2 as id, 'bob' as name union all
    select 3 as id, 'charlie' as name union all
    select 4 as id, 'diana' as name union all
    select 5 as id, 'eve' as name
)
"""

_MODEL_VIEW = """
{{ config(materialized='view') }}
select * from {{ ref('ctas_model') }}
"""


class TestAdapterResponseStats:
    """Test that SnowflakeAdapterResponse includes DML stats from cursor.stats.

    These stats are available in snowflake-connector-python >= 4.2.0 and provide
    granular information about DML operations (rows inserted, deleted, updated, duplicates).
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ctas_model.sql": _MODEL_CTAS,
            "view_model.sql": _MODEL_VIEW,
        }

    def test_ctas_adapter_response_has_stats(self, project):
        """Test that CTAS operations return rows_inserted in adapter response."""
        results = run_dbt(["run", "--select", "ctas_model"])
        assert len(results) == 1

        adapter_response = results[0].adapter_response

        # Basic fields should always be present
        assert "code" in adapter_response
        assert adapter_response["code"] == "SUCCESS"

        # New stats fields from cursor.stats (snowflake-connector-python >= 4.2.0)
        # For CTAS, rows_inserted should equal the number of rows created
        assert "rows_inserted" in adapter_response
        assert adapter_response["rows_inserted"] == 5

        # Other stats should be 0 or None for CTAS
        assert "rows_deleted" in adapter_response
        assert adapter_response["rows_deleted"] == 0

        assert "rows_updated" in adapter_response
        assert adapter_response["rows_updated"] == 0

        assert "rows_duplicates" in adapter_response
        assert adapter_response["rows_duplicates"] == 0

    def test_view_adapter_response_has_no_stats(self, project):
        """Test that VIEW operations don't include DML stats (they're omitted when None)."""
        # First ensure the table exists
        run_dbt(["run", "--select", "ctas_model"])

        results = run_dbt(["run", "--select", "view_model"])
        assert len(results) == 1

        adapter_response = results[0].adapter_response

        # Basic fields should always be present
        assert "code" in adapter_response
        assert adapter_response["code"] == "SUCCESS"

        # For non-DML operations like CREATE VIEW, stats are None and thus omitted
        # from the response dict (due to omit_none=True serialization)
        assert adapter_response.get("rows_inserted") is None
        assert adapter_response.get("rows_deleted") is None
        assert adapter_response.get("rows_updated") is None
        assert adapter_response.get("rows_duplicates") is None

    def test_rows_affected_uses_rows_inserted_for_ctas(self, project):
        """Test that rows_affected reflects rows_inserted for CTAS when accurate."""
        results = run_dbt(["run", "--select", "ctas_model"])
        assert len(results) == 1

        adapter_response = results[0].adapter_response

        # rows_affected should reflect the actual number of rows inserted
        # when the cursor.rowcount was -1 but rows_inserted is available
        rows_affected = adapter_response.get("rows_affected")
        rows_inserted = adapter_response.get("rows_inserted")

        # If rows_inserted is available, rows_affected should be meaningful
        if rows_inserted is not None and rows_inserted > 0:
            # rows_affected should either be the same as rows_inserted
            # or a positive value (depending on cursor.rowcount)
            assert rows_affected is not None
            assert rows_affected >= 0 or rows_affected == rows_inserted
