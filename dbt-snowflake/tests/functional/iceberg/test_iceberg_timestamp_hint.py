import pytest

from dbt.tests.util import run_dbt

# A v2 Iceberg table (the default) with a TIMESTAMP_NTZ(9) column.
# Snowflake always rejects nanosecond timestamps on v2 Iceberg tables (error 091385).
# The adapter should surface an actionable hint alongside the original error.
_MODEL_ICEBERG_NANO_TIMESTAMP = """
{{
  config(
    materialized="table",
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
  )
}}
select '2024-01-01'::TIMESTAMP_NTZ(9) as ts
"""


class TestIcebergTimestampNanosecondHint:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iceberg_nano_ts.sql": _MODEL_ICEBERG_NANO_TIMESTAMP,
        }

    def test_timestamp_ntz9_error_includes_actionable_hint(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        message = results[0].message
        # original Snowflake error is preserved
        assert "091385" in message
        # actionable hint is appended
        assert "TIMESTAMP_NTZ(6)" in message
        assert "nanosecond timestamp support enabled" in message
