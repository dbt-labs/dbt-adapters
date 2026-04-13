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
    MY_JS_UDF_WITH_DEFAULT_ARG_YML,
    MY_JS_UDF_WITH_SNOWFLAKE_CONFIGS_YML,
    MY_JS_UDF_YML,
    SUM_POSITIVE_JS,
    SUM_POSITIVE_JS_YML,
)


class JSUDFBase:
    """Base class for JavaScript UDF integration tests on BigQuery."""

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

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )


class TestBigQueryBasicJSUDF(JSUDFBase):
    """Test that a basic JavaScript scalar UDF can be created and executed on BigQuery."""

    def test_js_udf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Verify LANGUAGE js in generated SQL (BigQuery uses 'js', not 'JAVASCRIPT')
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE js" in sql

        # Verify no volatility clause by default
        assert "DETERMINISTIC" not in sql
        assert "NOT DETERMINISTIC" not in sql

        # Execute the function and verify the result
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert len(result.results) == 1
        select_value = result.results[0].agate_table.rows[0].values()[0]
        assert float(select_value) == 200.0


class TestBigQueryJSUDFMultiLineBody(JSUDFBase):
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
        assert "LANGUAGE js" in sql

        # Execute the function: mask_pii('hello') should return 'he***'
        result = run_dbt(["show", "--inline", "SELECT {{ function('mask_pii') }}('hello')"])
        assert len(result.results) == 1
        masked_value = result.results[0].agate_table.rows[0].values()[0]
        assert masked_value == "he***"


class TestBigQueryJSUDFDeterministicVolatility(JSUDFBase):
    """Test that deterministic volatility maps to DETERMINISTIC on BigQuery for JS UDFs.

    Per the RFC: deterministic -> BigQuery DETERMINISTIC (scalar only).
    """

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
        assert "LANGUAGE js" in sql
        assert "DETERMINISTIC" in sql
        # Must not be "NOT DETERMINISTIC"
        assert "NOT DETERMINISTIC" not in sql

        # Verify function still works
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class TestBigQueryJSUDFNonDeterministicVolatility(JSUDFBase):
    """Test that non-deterministic volatility maps to NOT DETERMINISTIC on BigQuery.

    Per the RFC: non-deterministic -> BigQuery NOT DETERMINISTIC (scalar only).
    """

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
        assert "LANGUAGE js" in sql
        assert "NOT DETERMINISTIC" in sql

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class TestBigQueryJSUDFStableVolatilityWarns(JSUDFBase):
    """Test that stable volatility is not supported on BigQuery and emits a warning.

    Per the RFC: stable -> not supported, warn and ignore.
    """

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
        assert "DETERMINISTIC" not in sql
        assert "NOT DETERMINISTIC" not in sql

        # Verify warning was emitted
        assert len(warning_event_catcher.caught_events) == 1
        assert (
            "Found `stable` volatility specified on function `price_for_xlarge`"
            in warning_event_catcher.caught_events[0].data.msg
        )

        # Function should still work
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class TestBigQueryJSUDFDefaultArgsNotSupported(JSUDFBase):
    """Test that default arguments are NOT supported for JS UDFs on BigQuery."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_WITH_DEFAULT_ARG_YML,
        }

    def test_js_udf_default_args_not_supported(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])
        assert len(result.results) == 1

        # BigQuery doesn't support default args — DEFAULT should not appear in SQL
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "DEFAULT 100" not in sql


class TestBigQueryJSUDFSnowflakeConfigsIgnored(JSUDFBase):
    """Test that Snowflake-specific configs (secure, null_handling, log_level, trace_level)
    are silently ignored on BigQuery.

    Per the RFC: adapter-specific configs silently ignored on unsupported adapters.
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_WITH_SNOWFLAKE_CONFIGS_YML,
        }

    def test_js_udf_snowflake_configs_ignored(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify Snowflake-specific clauses do NOT appear in BigQuery SQL
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE js" in sql
        assert "SECURE" not in sql
        assert "NULL ON NULL INPUT" not in sql
        assert "LOG_LEVEL" not in sql
        assert "TRACE_LEVEL" not in sql

        # Function should still work
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class TestBigQueryJSAggregateUDF:
    """Test that JavaScript aggregate UDFs work on BigQuery.

    Per the RFC, BigQuery supports JS UDAFs via CREATE AGGREGATE FUNCTION.
    The .js file must export: initialState, aggregate, merge, finalize.

    Expected DDL:
        CREATE OR REPLACE AGGREGATE FUNCTION dataset.sum_positive (x FLOAT64)
          RETURNS FLOAT64
          LANGUAGE js
        AS r'''
        export function initialState() { ... }
        ...
        ''';
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

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "sum_positive"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )

    def test_js_aggregate_udf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        # Find the function result (model may also be built)
        func_results = [r for r in result.results if r.node.name == "sum_positive"]
        assert len(func_results) == 1
        assert func_results[0].status == RunStatus.Success

        # Verify the generated SQL
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "CREATE OR REPLACE AGGREGATE FUNCTION" in sql
        assert "LANGUAGE js" in sql

        # Execute aggregate function: sum of positive values from basic_model (1 + 2 + 3 = 6)
        result = run_dbt(
            [
                "show",
                "--inline",
                "SELECT {{ function('sum_positive') }}(value) FROM {{ ref('basic_model') }}",
            ]
        )
        assert len(result.results) == 1
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 6.0


class TestBigQueryJSAggregateUDFVolatilityIgnored:
    """Test that volatility is ignored for BigQuery JS aggregate UDFs.

    Per the RFC: volatility is not supported for BigQuery UDAFs (ignored if set).
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

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "deterministic"},
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "sum_positive"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )

    def test_js_aggregate_udf_volatility_ignored(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        func_results = [r for r in result.results if r.node.name == "sum_positive"]
        assert len(func_results) == 1
        assert func_results[0].status == RunStatus.Success

        # Volatility should NOT appear for aggregate UDFs on BigQuery
        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "CREATE OR REPLACE AGGREGATE FUNCTION" in sql
        assert "DETERMINISTIC" not in sql
        assert "NOT DETERMINISTIC" not in sql
