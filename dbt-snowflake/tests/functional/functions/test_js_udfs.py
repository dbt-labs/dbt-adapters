import pytest

from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import JinjaLogWarning, RunResultError
from dbt.tests.adapter.functions.files import BASIC_MODEL_SQL
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher

from tests.functional.functions.files import (
    COMPUTE_TOTAL_JS_LOWERCASE,
    COMPUTE_TOTAL_JS_UPPERCASE,
    MASK_PII_JS,
    MASK_PII_JS_YML,
    MY_JS_UDF,
    MY_JS_UDF_ALL_CONFIGS_YML,
    MY_JS_UDF_LOG_LEVEL_YML,
    MY_JS_UDF_NULL_HANDLING_CALLED_YML,
    MY_JS_UDF_NULL_HANDLING_STRICT_YML,
    MY_JS_UDF_QUOTE_ARGS_FALSE_MULTI_ARG_YML,
    MY_JS_UDF_QUOTE_ARGS_TRUE_MULTI_ARG_YML,
    MY_JS_UDF_SECURE_YML,
    MY_JS_UDF_TRACE_LEVEL_YML,
    MY_JS_UDF_WITH_DEFAULT_ARG_YML,
    MY_JS_UDF_YML,
    SUM_POSITIVE_JS,
    SUM_POSITIVE_JS_YML,
)


class JSUDFBase:
    """Base class for JavaScript UDF integration tests on Snowflake."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    def is_function_alter_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "ALTER FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )

    @pytest.fixture(scope="class")
    def alter_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_alter_event(event),
        )


class TestSnowflakeBasicJSUDF(JSUDFBase):
    """Test that a basic JavaScript scalar UDF can be created and executed on Snowflake."""

    def test_js_udf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Verify LANGUAGE JAVASCRIPT in the generated SQL
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql

        # Verify $$ body delimiters (Snowflake convention)
        assert "$$" in sql

        # Verify no volatility clause by default
        assert "IMMUTABLE" not in sql
        assert "VOLATILE" not in sql

        # Execute the function and verify the result
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200


class TestSnowflakeJSUDFMultiLineBody(JSUDFBase):
    """Test a more complex JS UDF with multi-line logic (if/else, loops)."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "mask_pii.js": MASK_PII_JS,
            "mask_pii.yml": MASK_PII_JS_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "mask_pii"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    def test_js_udf_multi_line(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql

        # Execute the function: mask_pii('hello') should return 'he***'
        result = run_dbt(["show", "--inline", "SELECT {{ function('mask_pii') }}('hello')"])
        assert len(result.results) == 1
        masked_value = result.results[0].agate_table.rows[0].values()[0]
        assert masked_value == "he***"


class TestSnowflakeJSUDFDeterministicVolatility(JSUDFBase):
    """Test that deterministic volatility maps to IMMUTABLE on Snowflake for JS UDFs."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "deterministic"},
        }

    def test_js_udf_deterministic(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql
        assert "IMMUTABLE" in sql

        # Verify function still works
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestSnowflakeJSUDFNonDeterministicVolatility(JSUDFBase):
    """Test that non-deterministic volatility maps to VOLATILE on Snowflake for JS UDFs."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "non-deterministic"},
        }

    def test_js_udf_non_deterministic(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql
        assert "VOLATILE" in sql

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestSnowflakeJSUDFStableVolatility(JSUDFBase):
    """Test that stable volatility is not supported on Snowflake and emits a warning."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "stable"},
        }

    def test_js_udf_stable_warns(self, project, sql_event_catcher):
        warning_event_catcher = EventCatcher(JinjaLogWarning)
        result = run_dbt(
            ["build", "--debug"],
            callbacks=[sql_event_catcher.catch, warning_event_catcher.catch],
        )

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify no volatility clause in generated SQL
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "IMMUTABLE" not in sql
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql

        # Verify warning was emitted
        assert len(warning_event_catcher.caught_events) == 1
        assert (
            "Found `stable` volatility specified on function `price_for_xlarge`. "
            "This volatility is not supported by snowflake, and will be ignored"
            in warning_event_catcher.caught_events[0].data.msg
        )

        # Function should still work
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestSnowflakeJSUDFDefaultArgs(JSUDFBase):
    """Test that default arguments are supported for JS UDFs on Snowflake."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_WITH_DEFAULT_ARG_YML,
        }

    def test_js_udf_default_args(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify DEFAULT in generated SQL
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "DEFAULT 100" in sql
        assert "LANGUAGE JAVASCRIPT" in sql

        # Call with no args — should use default value of 100
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}()"])
        assert len(result.results) == 1
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestSnowflakeJSUDFQuoteArgsTrue:
    """Test that quote_args: true (the default) double-quotes all args for case preservation."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "compute_total.js": COMPUTE_TOTAL_JS_LOWERCASE,
            "compute_total.yml": MY_JS_UDF_QUOTE_ARGS_TRUE_MULTI_ARG_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "compute_total"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )

    def test_js_udf_quote_args_true(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql

        # Both args should be double-quoted
        assert '"price"' in sql
        assert '"quantity"' in sql

        # Execute: 10 * 5 = 50
        result = run_dbt(["show", "--inline", "SELECT {{ function('compute_total') }}(10, 5)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 50


class TestSnowflakeJSUDFQuoteArgsFalse:
    """Test that quote_args: false emits unquoted args."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "compute_total.js": COMPUTE_TOTAL_JS_UPPERCASE,
            "compute_total.yml": MY_JS_UDF_QUOTE_ARGS_FALSE_MULTI_ARG_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "compute_total"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )

    def test_js_udf_quote_args_false(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql

        # Neither arg should be quoted
        assert '"price"' not in sql
        assert '"quantity"' not in sql

        # Execute: 10 * 5 = 50 — JS body uses PRICE * QUANTITY (uppercase)
        result = run_dbt(["show", "--inline", "SELECT {{ function('compute_total') }}(10, 5)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 50


class TestSnowflakeJSAggregateUDFError:
    """Test that JavaScript aggregate UDFs error on Snowflake."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_positive.js": SUM_POSITIVE_JS,
            "sum_positive.yml": SUM_POSITIVE_JS_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": BASIC_MODEL_SQL,
        }

    def test_js_aggregate_udf_errors(self, project):
        run_result_error_catcher = EventCatcher(RunResultError)
        result = run_dbt(
            ["build", "--debug"],
            expect_pass=False,
            callbacks=[run_result_error_catcher.catch],
        )

        # The JS aggregate function should fail; the model may or may not be built
        js_func_results = [r for r in result.results if r.node.name == "sum_positive"]
        assert len(js_func_results) == 1
        assert js_func_results[0].status == RunStatus.Error

        # Verify the error message mentions JS aggregate UDFs not being supported
        assert len(run_result_error_catcher.caught_events) >= 1
        error_msgs = [e.data.msg for e in run_result_error_catcher.caught_events]
        assert any(
            "not supported" in msg.lower() or "aggregate_function_javascript" in msg.lower()
            for msg in error_msgs
        )
