import pytest

from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import GenericExceptionOnRun
from dbt.tests.adapter.functions import files
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher


class UDFsBasic:

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": files.MY_UDF_SQL,
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR REPLACE FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery, predicate=lambda event: self.is_function_create_event(event)
        )

    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql

    def test_udfs(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        node = node_result.node
        assert isinstance(node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Check volatility
        assert len(sql_event_catcher.caught_events) == 1
        self.check_function_volatility(sql_event_catcher.caught_events[0].data.sql)

        # Check that the function can be executed
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        # The result should have an agate table with one row and one column (and thus only one value, which is our inline selection)
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200  # the UDF should return 2x the input value (100 * 2 = 200)


class DeterministicUDF(UDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "deterministic"},
        }

    def check_function_volatility(self, sql: str):
        assert "IMMUTABLE" in sql


class StableUDF(UDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "stable"},
        }

    def check_function_volatility(self, sql: str):
        assert "STABLE" in sql


class NonDeterministicUDF(UDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "non-deterministic"},
        }

    def check_function_volatility(self, sql: str):
        assert "VOLATILE" in sql


class ErrorForUnsupportedType(UDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+type": "table"},
        }

    def test_udfs(self, project, sql_event_catcher):
        generic_exception_catcher = EventCatcher(GenericExceptionOnRun)
        result = run_dbt(
            ["build", "--debug"], expect_pass=False, callbacks=[generic_exception_catcher.catch]
        )
        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Error

        assert len(generic_exception_catcher.caught_events) == 1
        assert (
            "sql table function not implemented for adapter"
            in generic_exception_catcher.caught_events[0].data.exc
        )
