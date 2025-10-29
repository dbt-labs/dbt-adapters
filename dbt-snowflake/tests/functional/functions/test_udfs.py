import pytest
from dbt.contracts.results import RunStatus
from dbt.tests.adapter.functions.files import MY_UDF_YML
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
)
from dbt.tests.util import run_dbt
from tests.functional.functions.files import MY_UDF_SQL


class TestSnowflakeUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestSnowflakeDeterministicUDFs(DeterministicUDF):
    pass


class TestSnowflakeStableUDFs(StableUDF):
    def test_udfs(self, project, sql_event_catcher):
        result = run_dbt(
            ["build", "--debug"], expect_pass=False, callbacks=[sql_event_catcher.catch]
        )
        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Error
        assert (
            "`Stable` function volatility is not supported for Snowflake"
            in result.results[0].message
        )


class TestSnowflakeNonDeterministicUDFs(NonDeterministicUDF):
    pass
