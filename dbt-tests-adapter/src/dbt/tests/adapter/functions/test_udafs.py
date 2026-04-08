import pytest

from dbt.adapters.events.types import SQLQuery
from dbt.tests.adapter.functions import files
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher


class UDAFBase:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": files.BASIC_MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def functions(self):
        raise NotImplementedError("Test subclass must implement functions")

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "sum_squared"
            and "CREATE OR REPLACE AGGREGATE FUNCTION" in event.data.sql
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

    def test_udaf(self, project, sql_event_catcher):
        # build the function
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        # Check volatility
        assert len(sql_event_catcher.caught_events) == 1
        self.check_function_volatility(sql_event_catcher.caught_events[0].data.sql)

        # try using the aggregate function
        result = run_dbt(
            [
                "show",
                "--inline",
                "SELECT {{ function('sum_squared') }}(value) FROM {{ ref('basic_model') }}",
            ]
        )
        assert len(result.results) == 1
        # 1 + 2 + 3 = 6, then 6^2 = 36
        assert result.results[0].agate_table.rows[0].values()[0] == 36.0


class BasicPythonUDAF(UDAFBase):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_squared.py": files.SUM_SQUARED_UDAF_PYTHON,
            "sum_squared.yml": files.SUM_SQUARED_UDAF_PYTHON_YML,
        }


class BasicSQLUDAF(UDAFBase):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_squared.sql": files.SUM_SQUARED_UDAF_SQL,
            "sum_squared.yml": files.SUM_SQUARED_UDAF_SQL_YML,
        }


class PythonUDAFDefaultArgSupport(BasicPythonUDAF):
    expect_default_arg_support = False

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_squared.py": files.SUM_SQUARED_UDAF_PYTHON,
            "sum_squared.yml": files.SUM_SQUARED_UDAF_PYTHON_WITH_DEFAULT_ARG_YML,
        }

    def test_udaf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])
        assert len(result.results) == 2

        if not self.expect_default_arg_support:
            assert "DEFAULT 1" not in sql_event_catcher.caught_events[0].data.sql
        else:
            assert "DEFAULT 1" in sql_event_catcher.caught_events[0].data.sql

            result = run_dbt(
                [
                    "show",
                    "--inline",
                    "SELECT {{ function('sum_squared') }}() FROM {{ ref('basic_model') }}",
                ]
            )
            assert len(result.results) == 1
            assert result.results[0].agate_table.rows[0].values()[0] == 9.0
