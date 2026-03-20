import pytest

from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.adapter.functions import files
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher


class TVFBase:

    @pytest.fixture(scope="class")
    def functions(self):
        raise NotImplementedError("Test subclass must implement functions")

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "generate_double_series"
            and "CREATE OR REPLACE TABLE FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery, predicate=lambda event: self.is_function_create_event(event)
        )

    def test_tvf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        node = node_result.node
        assert isinstance(node, FunctionNode)
        assert node_result.node.name == "generate_double_series"

        # Check that it created a TABLE FUNCTION
        assert len(sql_event_catcher.caught_events) == 1
        assert "TABLE FUNCTION" in sql_event_catcher.caught_events[0].data.sql

        # Check that the function can be executed
        result = run_dbt(
            ["show", "--inline", "SELECT * FROM {{ function('generate_double_series') }}(5)"]
        )
        assert len(result.results) == 1
        # Should return 5 rows: (1,2), (2,4), (3,6), (4,8), (5,10)
        assert len(result.results[0].agate_table.rows) == 5


class BasicSQLTVF(TVFBase):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "generate_double_series.sql": files.MY_TVF_SQL,
            "generate_double_series.yml": files.MY_TVF_YML,
        }
