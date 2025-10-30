import pytest
from dbt.contracts.graph.nodes import FunctionNode
from dbt.contracts.results import RunStatus
from dbt.events.types import JinjaLogWarning
from dbt.tests.adapter.functions.files import MY_UDF_YML
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
)
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
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
    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql

    def test_udfs(self, project, sql_event_catcher):
        warning_event_catcher = EventCatcher(JinjaLogWarning)
        result = run_dbt(
            ["build", "--debug"], callbacks=[sql_event_catcher.catch, warning_event_catcher.catch]
        )

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        node = node_result.node
        assert isinstance(node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Check volatility
        assert len(sql_event_catcher.caught_events) == 1
        self.check_function_volatility(sql_event_catcher.caught_events[0].data.sql)

        # Check that the warning event was caught
        assert len(warning_event_catcher.caught_events) == 1
        assert (
            "`Stable` function volatility is not supported by Snowflake, and will be ignored"
            in warning_event_catcher.caught_events[0].data.msg
        )

        # Check that the function can be executed
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        # The result should have an agate table with one row and one column (and thus only one value, which is our inline selection)
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200  # the UDF should return 2x the input value (100 * 2 = 200)


class TestSnowflakeNonDeterministicUDFs(NonDeterministicUDF):
    pass
