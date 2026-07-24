import pytest

from dbt.tests.util import run_dbt


# A contract that declares a bare NUMBER (no precision/scale) on an Iceberg table.
# The guard should fail this at compile time, before any DDL reaches Snowflake.
_MODEL_BARE_NUMBER = """
{{
  config(
    materialized="table",
    table_format="iceberg",
    contract={"enforced": true},
  )
}}
select 1::number as id
"""

_SCHEMA_BARE_NUMBER = """
version: 2
models:
  - name: iceberg_bare_number
    columns:
      - name: id
        data_type: number
"""

# The same shape with an explicit precision/scale must NOT trip the guard.
_MODEL_EXPLICIT_NUMBER = """
{{
  config(
    materialized="table",
    table_format="iceberg",
    contract={"enforced": true},
  )
}}
select 1::number(38, 0) as id
"""

_SCHEMA_EXPLICIT_NUMBER = """
version: 2
models:
  - name: iceberg_explicit_number
    columns:
      - name: id
        data_type: number(38, 0)
"""


class TestIcebergContractBareNumericRaises:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iceberg_bare_number.sql": _MODEL_BARE_NUMBER,
            "schema.yml": _SCHEMA_BARE_NUMBER,
        }

    def test_bare_number_contract_raises_actionable_error(self, project):
        result = run_dbt(["run"], expect_pass=False)
        assert len(result) == 1
        message = result[0].message
        assert "099200" in message
        assert "number(38, 0)" in message
        assert "id" in message


class TestIcebergContractExplicitNumericPasses:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iceberg_explicit_number.sql": _MODEL_EXPLICIT_NUMBER,
            "schema.yml": _SCHEMA_EXPLICIT_NUMBER,
        }

    def test_explicit_precision_does_not_trip_the_guard(self, project):
        # With explicit precision the guard must not fire; the model builds normally.
        run_dbt(["run"])
