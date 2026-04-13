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
    MASK_PII_JS,
    MASK_PII_JS_YML,
    MY_JS_UDF,
    MY_JS_UDF_ALL_CONFIGS_YML,
    MY_JS_UDF_LOG_LEVEL_YML,
    MY_JS_UDF_NULL_HANDLING_CALLED_YML,
    MY_JS_UDF_NULL_HANDLING_STRICT_YML,
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


class TestSnowflakeJSUDFSecure(JSUDFBase):
    """Test that secure: true adds the SECURE modifier to CREATE FUNCTION."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_SECURE_YML,
        }

    def test_js_udf_secure(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql
        assert "CREATE OR REPLACE SECURE FUNCTION" in sql

        # Function should still work
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestSnowflakeJSUDFNullHandlingStrict(JSUDFBase):
    """Test that null_handling: strict generates RETURNS NULL ON NULL INPUT.

    Per the RFC, this tells Snowflake to skip JS execution entirely for NULL rows.
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_NULL_HANDLING_STRICT_YML,
        }

    def test_js_udf_null_handling_strict(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql
        assert "RETURNS NULL ON NULL INPUT" in sql

        # When called with NULL, function should return NULL without executing JS
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(NULL)"])
        assert len(result.results) == 1
        assert result.results[0].agate_table.rows[0].values()[0] is None


class TestSnowflakeJSUDFNullHandlingCalled(JSUDFBase):
    """Test that null_handling: called generates CALLED ON NULL INPUT.

    This is the default behavior — JS code receives NULL and handles it.
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_NULL_HANDLING_CALLED_YML,
        }

    def test_js_udf_null_handling_called(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql
        assert "CALLED ON NULL INPUT" in sql


class TestSnowflakeJSUDFLogLevel(JSUDFBase):
    """Test that log_level config generates an ALTER FUNCTION with SET LOG_LEVEL.

    Per the RFC, log_level is set via ALTER FUNCTION after creation:
        ALTER FUNCTION schema.fn(float) SET LOG_LEVEL = 'INFO';
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_LOG_LEVEL_YML,
        }

    def test_js_udf_log_level(self, project, sql_event_catcher, alter_event_catcher):
        result = run_dbt(
            ["build", "--debug"],
            callbacks=[sql_event_catcher.catch, alter_event_catcher.catch],
        )

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify the CREATE statement
        assert len(sql_event_catcher.caught_events) == 1
        create_sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in create_sql

        # Verify ALTER FUNCTION for log_level
        assert len(alter_event_catcher.caught_events) >= 1
        alter_sqls = [e.data.sql for e in alter_event_catcher.caught_events]
        assert any("SET LOG_LEVEL" in sql and "'INFO'" in sql for sql in alter_sqls)


class TestSnowflakeJSUDFTraceLevel(JSUDFBase):
    """Test that trace_level config generates an ALTER FUNCTION with SET TRACE_LEVEL.

    Per the RFC, trace_level is set via ALTER FUNCTION after creation:
        ALTER FUNCTION schema.fn(float) SET TRACE_LEVEL = 'OFF';
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_TRACE_LEVEL_YML,
        }

    def test_js_udf_trace_level(self, project, sql_event_catcher, alter_event_catcher):
        result = run_dbt(
            ["build", "--debug"],
            callbacks=[sql_event_catcher.catch, alter_event_catcher.catch],
        )

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify the CREATE statement
        assert len(sql_event_catcher.caught_events) == 1
        create_sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in create_sql

        # Verify ALTER FUNCTION for trace_level
        assert len(alter_event_catcher.caught_events) >= 1
        alter_sqls = [e.data.sql for e in alter_event_catcher.caught_events]
        assert any("SET TRACE_LEVEL" in sql and "'OFF'" in sql for sql in alter_sqls)


class TestSnowflakeJSUDFAllConfigs(JSUDFBase):
    """Test a JS UDF with all Snowflake-specific config options set together"""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_ALL_CONFIGS_YML,
        }

    def test_js_udf_all_configs(self, project, sql_event_catcher, alter_event_catcher):
        result = run_dbt(
            ["build", "--debug"],
            callbacks=[sql_event_catcher.catch, alter_event_catcher.catch],
        )

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify the CREATE statement has all expected clauses
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "CREATE OR REPLACE SECURE FUNCTION" in sql
        assert "LANGUAGE JAVASCRIPT" in sql
        assert "IMMUTABLE" in sql
        assert "RETURNS NULL ON NULL INPUT" in sql
        assert "DEFAULT 100" in sql
        assert "$$" in sql

        # Verify ALTER statements for log_level and trace_level
        assert len(alter_event_catcher.caught_events) >= 2
        alter_sqls = [e.data.sql for e in alter_event_catcher.caught_events]
        assert any("SET LOG_LEVEL" in sql and "'INFO'" in sql for sql in alter_sqls)
        assert any("SET TRACE_LEVEL" in sql and "'OFF'" in sql for sql in alter_sqls)

        # Execute with default arg
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}()"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200

        # Execute with explicit arg
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(50)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 100


class TestSnowflakeJSUDFArgumentQuoting(JSUDFBase):
    """Test that JS UDF arguments are double-quoted on Snowflake for case preservation."""

    def test_js_udf_argument_quoting(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE JAVASCRIPT" in sql

        # Arguments should be double-quoted for case preservation
        assert '"price"' in sql

        # Verify function works with lowercase arg in JS body
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestSnowflakeJSAggregateUDFError:
    """Test that JavaScript aggregate UDFs error on Snowflake.

    Per the RFC, Snowflake does NOT support JavaScript UDAFs. Attempting to create one
    should produce a clear error at compile time.
    """

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
