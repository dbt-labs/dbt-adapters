import pytest

from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import JinjaLogWarning
from dbt.tests.adapter.functions import files
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher


class JSUDFBase:
    """Fixture-only base class for JavaScript scalar UDF tests."""

    expected_language_keyword = ""
    expected_body_delimiter = ""

    @pytest.fixture(scope="class")
    def functions(self):
        raise NotImplementedError("Subclass must provide functions fixture")

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

    def check_function_volatility(self, sql: str):
        """Override to assert adapter-specific volatility keywords."""
        raise NotImplementedError("Subclass must implement check_function_volatility")


class BasicJSUDF(JSUDFBase):
    """Test basic JS UDF creation and execution."""

    def test_js_udf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert self.expected_language_keyword is not None
        assert self.expected_language_keyword in sql

        assert self.expected_body_delimiter is not None
        assert self.expected_body_delimiter in sql

        self.check_function_volatility(sql)

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert len(result.results) == 1
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class JSUDFMultiLineBody:
    """Test a JS UDF with multi-line body (mask_pii)."""

    expected_language_keyword = ""

    @pytest.fixture(scope="class")
    def functions(self):
        raise NotImplementedError("Subclass must provide functions fixture")

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "mask_pii"
            and "CREATE OR REPLACE" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )

    def test_js_udf_multi_line(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql

        assert self.expected_language_keyword is not None
        assert self.expected_language_keyword in sql

        result = run_dbt(["show", "--inline", "SELECT {{ function('mask_pii') }}('hello')"])
        assert len(result.results) == 1
        assert result.results[0].agate_table.rows[0].values()[0] == "he***"


class JSUDFDeterministicVolatility(JSUDFBase):
    """Test deterministic volatility for JS UDFs."""

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

        assert self.expected_language_keyword is not None
        assert self.expected_language_keyword in sql

        self.check_function_volatility(sql)

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class JSUDFNonDeterministicVolatility(JSUDFBase):
    """Test non-deterministic volatility for JS UDFs."""

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

        assert self.expected_language_keyword is not None
        assert self.expected_language_keyword in sql

        self.check_function_volatility(sql)

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class JSUDFStableVolatilityWarns(JSUDFBase):
    """Test that stable volatility emits a warning and is excluded from SQL."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "stable"},
        }

    def check_no_volatility_in_sql(self, sql: str):
        """Override to assert no volatility keywords in SQL."""
        raise NotImplementedError("Subclass must implement check_no_volatility_in_sql")

    def check_function_volatility(self, sql: str):
        self.check_no_volatility_in_sql(sql)

    def test_js_udf_stable_warns(self, project, sql_event_catcher):
        warning_event_catcher = EventCatcher(JinjaLogWarning)
        result = run_dbt(
            ["build", "--debug"],
            callbacks=[sql_event_catcher.catch, warning_event_catcher.catch],
        )

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        self.check_no_volatility_in_sql(sql)

        assert len(warning_event_catcher.caught_events) == 1
        assert (
            "Found `stable` volatility specified on function `price_for_xlarge`"
            in warning_event_catcher.caught_events[0].data.msg
        )

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"])
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class JSUDFDefaultArgSupport(JSUDFBase):
    """Test default argument support for JS UDFs."""

    expect_default_arg_support = False

    def test_js_udf_default_args(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])
        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql

        if not self.expect_default_arg_support:
            assert "DEFAULT 100" not in sql
            result = run_dbt(
                ["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100.0)"]
            )
            assert len(result.results) == 1
            assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0
        else:
            assert "DEFAULT 100" in sql
            if self.expected_language_keyword:
                assert self.expected_language_keyword in sql
            result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}()"])
            assert len(result.results) == 1
            assert float(result.results[0].agate_table.rows[0].values()[0]) == 200.0


class JSAggregateUDFBase:
    """Fixture-only base class for JS aggregate UDF tests."""

    expected_language_keyword = ""

    @pytest.fixture(scope="class")
    def functions(self):
        raise NotImplementedError("Subclass must provide functions fixture")

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": files.BASIC_MODEL_SQL,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "sum_positive"
            and "CREATE OR REPLACE AGGREGATE FUNCTION" in event.data.sql
            and "FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: self.is_function_create_event(event),
        )


class BasicJSAggregateUDF(JSAggregateUDFBase):
    """Test JS aggregate UDF creation and execution (for adapters that support them)."""

    def test_js_aggregate_udf(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        func_results = [r for r in result.results if r.node.name == "sum_positive"]
        assert len(func_results) == 1
        assert func_results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "CREATE OR REPLACE AGGREGATE FUNCTION" in sql

        assert self.expected_language_keyword is not None
        assert self.expected_language_keyword in sql

        result = run_dbt(
            [
                "show",
                "--inline",
                "SELECT {{ function('sum_positive') }}(value) FROM {{ ref('basic_model') }}",
            ]
        )
        assert len(result.results) == 1
        assert float(result.results[0].agate_table.rows[0].values()[0]) == 6.0


class JSAggregateUDFVolatilityIgnored(JSAggregateUDFBase):
    """Test that volatility is ignored for JS aggregate UDFs."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "deterministic"},
        }

    def check_no_volatility_in_sql(self, sql: str):
        """Override to assert no volatility keywords in aggregate SQL."""
        raise NotImplementedError("Subclass must implement check_no_volatility_in_sql")

    def test_js_aggregate_udf_volatility_ignored(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        func_results = [r for r in result.results if r.node.name == "sum_positive"]
        assert len(func_results) == 1
        assert func_results[0].status == RunStatus.Success

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "CREATE OR REPLACE AGGREGATE FUNCTION" in sql
        self.check_no_volatility_in_sql(sql)
