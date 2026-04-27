import pytest

from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.exceptions import ParsingError
from dbt.tests.adapter.functions.test_js_udfs import (
    BasicJSAggregateUDF,
    BasicJSUDF,
    JSUDFDefaultArgSupport,
    JSUDFDeterministicVolatility,
    JSUDFMultiLineBody,
    JSUDFNonDeterministicVolatility,
    JSUDFStableVolatilityWarns,
)
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher

from tests.functional.functions.files import (
    COMPUTE_TOTAL_JS_LOWERCASE,
    COMPUTE_TOTAL_JS_UPPERCASE,
    MASK_PII_JS,
    MASK_PII_JS_YML,
    MY_JS_UDF,
    MY_JS_UDF_QUOTE_ARGS_FALSE_MULTI_ARG_YML,
    MY_JS_UDF_QUOTE_ARGS_TRUE_MULTI_ARG_YML,
    MY_JS_UDF_WITH_DEFAULT_ARG_YML,
    MY_JS_UDF_YML,
    SUM_POSITIVE_JS,
    SUM_POSITIVE_JS_YML,
)


class TestSnowflakeBasicJSUDF(BasicJSUDF):
    """Test that a basic JavaScript scalar UDF can be created and executed on Snowflake."""

    expected_language_keyword = "LANGUAGE JAVASCRIPT"
    expected_body_delimiter = "$$"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_function_volatility(self, sql):
        assert "IMMUTABLE" not in sql
        assert "VOLATILE" not in sql


class TestSnowflakeJSUDFMultiLineBody(JSUDFMultiLineBody):
    """Test a more complex JS UDF with multi-line logic (if/else, loops)."""

    expected_language_keyword = "LANGUAGE JAVASCRIPT"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "mask_pii.js": MASK_PII_JS,
            "mask_pii.yml": MASK_PII_JS_YML,
        }


class TestSnowflakeJSUDFDeterministicVolatility(JSUDFDeterministicVolatility):
    """Test that deterministic volatility maps to IMMUTABLE on Snowflake for JS UDFs."""

    expected_language_keyword = "LANGUAGE JAVASCRIPT"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_function_volatility(self, sql):
        assert "IMMUTABLE" in sql


class TestSnowflakeJSUDFNonDeterministicVolatility(JSUDFNonDeterministicVolatility):
    """Test that non-deterministic volatility maps to VOLATILE on Snowflake for JS UDFs."""

    expected_language_keyword = "LANGUAGE JAVASCRIPT"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_function_volatility(self, sql):
        assert "VOLATILE" in sql


class TestSnowflakeJSUDFStableVolatility(JSUDFStableVolatilityWarns):
    """Test that stable volatility is not supported on Snowflake and emits a warning."""

    expected_language_keyword = "LANGUAGE JAVASCRIPT"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_no_volatility_in_sql(self, sql):
        assert "IMMUTABLE" not in sql
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql


class TestSnowflakeJSUDFDefaultArgs(JSUDFDefaultArgSupport):
    """Test that default arguments are supported for JS UDFs on Snowflake."""

    expect_default_arg_support = True
    expected_language_keyword = "LANGUAGE JAVASCRIPT"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_WITH_DEFAULT_ARG_YML,
        }

    def check_function_volatility(self, sql):
        pass  # Not checked in default args test


# --- Snowflake-specific tests (not in shared base classes) ---


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
    """Test that JavaScript aggregate UDFs raise a parsing error on Snowflake."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_positive.js": SUM_POSITIVE_JS,
            "sum_positive.yml": SUM_POSITIVE_JS_YML,
        }

    def test_js_aggregate_udf_errors(self, project):
        with pytest.raises(ParsingError):
            run_dbt(["build"])
