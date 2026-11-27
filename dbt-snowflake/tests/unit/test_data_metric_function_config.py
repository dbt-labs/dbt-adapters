"""
Unit tests for the data metric function (DMF) config and diff logic:
- parsing the model config into SnowflakeDataMetricFunctionsConfig
- parsing Snowflake introspection results
- diffing desired vs. existing into a changeset (add / drop / schedule)
- config validation surfaced from parse_model
"""

import agate
import pytest

from dbt_common.exceptions import CompilationError, DbtConfigError

from dbt.adapters.snowflake import SnowflakeRelation
from dbt.adapters.snowflake.parse_model import data_metric_functions, data_metric_schedule
from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDataMetricFunction,
    SnowflakeDataMetricFunctionsConfig,
)


REFERENCE_COLUMNS = [
    "metric_database_name",
    "metric_schema_name",
    "metric_name",
    "ref_arguments",
    "schedule",
    "schedule_status",
]


def _references(*rows) -> dict:
    """Build the RelationResults dict produced by describe_data_metric_functions.

    Each row is a dict keyed on a subset of the reference columns; ``ref_arguments``
    is passed as a JSON string, matching how Snowflake surfaces the ARRAY column.
    """
    table = agate.Table(
        [[row.get(column) for column in REFERENCE_COLUMNS] for row in rows],
        REFERENCE_COLUMNS,
        [agate.Text()] * len(REFERENCE_COLUMNS),
    )
    return {"data_metric_function_references": table}


def _ref_arguments(*columns) -> str:
    import json

    return json.dumps([{"name": column, "domain": "COLUMN"} for column in columns])


class _FakeConfig:
    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _FakeModel:
    """Stands in for a RelationConfig; only ``.config.get`` is exercised."""

    def __init__(self, **values):
        self.config = _FakeConfig(values)


class TestSnowflakeDataMetricFunction:
    def test_comparison_key_is_case_insensitive(self):
        lower = SnowflakeDataMetricFunction(
            name="snowflake.core.null_count", arguments=("trade_id",)
        )
        upper = SnowflakeDataMetricFunction(
            name="SNOWFLAKE.CORE.NULL_COUNT", arguments=("TRADE_ID",)
        )
        assert lower.comparison_key == upper.comparison_key

    def test_comparison_key_preserves_quoted_identifiers(self):
        dmf = SnowflakeDataMetricFunction(
            name="snowflake.core.null_count", arguments=('"TradeId"',)
        )
        assert dmf.comparison_key == ("SNOWFLAKE.CORE.NULL_COUNT", ("TradeId",))

    def test_arguments_clause_preserves_order_and_text(self):
        dmf = SnowflakeDataMetricFunction(
            name="snowflake.core.duplicate_count", arguments=("trade_id", "region")
        )
        assert dmf.arguments_clause == "trade_id, region"


class TestSnowflakeDataMetricFunctionsConfig:
    def test_from_dict_builds_metric_functions(self):
        config = SnowflakeDataMetricFunctionsConfig.from_dict(
            {
                "schedule": "5 MINUTE",
                "metric_functions": [
                    {"name": "snowflake.core.null_count", "arguments": ["trade_id"]},
                ],
            }
        )
        assert config.schedule == "5 MINUTE"
        assert config.metric_functions == (
            SnowflakeDataMetricFunction(name="snowflake.core.null_count", arguments=("trade_id",)),
        )

    def test_from_relation_config_requires_schedule_when_functions_present(self):
        model = _FakeModel(
            data_metric_functions=[
                {"metric": "snowflake.core.null_count", "expression": "trade_id"}
            ]
        )
        with pytest.raises(CompilationError, match="data_metric_schedule` is required"):
            SnowflakeDataMetricFunctionsConfig.from_relation_config(model)

    def test_from_relation_config_allows_schedule_without_functions(self):
        model = _FakeModel(data_metric_schedule="5 MINUTE")
        config = SnowflakeDataMetricFunctionsConfig.from_relation_config(model)
        assert config.schedule == "5 MINUTE"
        assert config.metric_functions == ()

    def test_from_relation_results_parses_schedule_and_arguments(self):
        results = _references(
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "NULL_COUNT",
                "ref_arguments": _ref_arguments("TRADE_ID"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            },
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "DUPLICATE_COUNT",
                "ref_arguments": _ref_arguments("TRADE_ID", "REGION"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            },
        )
        config = SnowflakeDataMetricFunctionsConfig.from_relation_results(results)
        assert config.schedule == "5 MINUTE"
        assert config.metric_functions == (
            SnowflakeDataMetricFunction(name="SNOWFLAKE.CORE.NULL_COUNT", arguments=("TRADE_ID",)),
            SnowflakeDataMetricFunction(
                name="SNOWFLAKE.CORE.DUPLICATE_COUNT", arguments=("TRADE_ID", "REGION")
            ),
        )

    def test_from_relation_results_handles_no_attached_dmfs(self):
        config = SnowflakeDataMetricFunctionsConfig.from_relation_results(_references())
        assert config.schedule is None
        assert config.metric_functions == ()


class TestDataMetricFunctionChangeset:
    def test_adds_configured_functions_when_none_exist(self):
        model = _FakeModel(
            data_metric_schedule="5 MINUTE",
            data_metric_functions=[
                {"metric": "snowflake.core.null_count", "expression": "trade_id"},
                {"metric": "snowflake.core.duplicate_count", "expression": ["trade_id", "region"]},
            ],
        )
        changeset = SnowflakeRelation.data_metric_function_changeset(_references(), model)

        assert changeset is not None
        assert changeset.schedule == "5 MINUTE"
        assert [dmf.name for dmf in changeset.to_add] == [
            "snowflake.core.null_count",
            "snowflake.core.duplicate_count",
        ]
        assert changeset.to_add[1].arguments == ("trade_id", "region")
        assert changeset.to_drop == ()

    def test_no_changes_when_config_matches_snowflake(self):
        model = _FakeModel(
            data_metric_schedule="5 MINUTE",
            data_metric_functions=[
                {"metric": "snowflake.core.null_count", "expression": "trade_id"},
            ],
        )
        results = _references(
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "NULL_COUNT",
                "ref_arguments": _ref_arguments("TRADE_ID"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            }
        )
        assert SnowflakeRelation.data_metric_function_changeset(results, model) is None

    def test_drops_functions_not_in_config(self):
        model = _FakeModel(
            data_metric_schedule="5 MINUTE",
            data_metric_functions=[
                {"metric": "snowflake.core.null_count", "expression": "trade_id"},
            ],
        )
        results = _references(
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "NULL_COUNT",
                "ref_arguments": _ref_arguments("TRADE_ID"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            },
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "ROW_COUNT",
                "ref_arguments": _ref_arguments(),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            },
        )
        changeset = SnowflakeRelation.data_metric_function_changeset(results, model)

        assert changeset is not None
        assert changeset.schedule is None  # unchanged
        assert changeset.to_add == ()
        assert [dmf.name for dmf in changeset.to_drop] == ["SNOWFLAKE.CORE.ROW_COUNT"]

    def test_empty_config_detaches_all_managed_functions(self):
        model = _FakeModel()
        results = _references(
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "NULL_COUNT",
                "ref_arguments": _ref_arguments("TRADE_ID"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            }
        )
        changeset = SnowflakeRelation.data_metric_function_changeset(results, model)

        assert changeset is not None
        assert [dmf.name for dmf in changeset.to_drop] == ["SNOWFLAKE.CORE.NULL_COUNT"]

    def test_schedule_change_only(self):
        model = _FakeModel(
            data_metric_schedule="USING CRON 0 6 * * * UTC",
            data_metric_functions=[
                {"metric": "snowflake.core.null_count", "expression": "trade_id"},
            ],
        )
        results = _references(
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "NULL_COUNT",
                "ref_arguments": _ref_arguments("TRADE_ID"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            }
        )
        changeset = SnowflakeRelation.data_metric_function_changeset(results, model)

        assert changeset is not None
        assert changeset.schedule == "USING CRON 0 6 * * * UTC"
        assert changeset.to_add == ()
        assert changeset.to_drop == ()

    def test_argument_order_is_significant(self):
        model = _FakeModel(
            data_metric_schedule="5 MINUTE",
            data_metric_functions=[
                {"metric": "snowflake.core.duplicate_count", "expression": ["trade_id", "region"]},
            ],
        )
        results = _references(
            {
                "metric_database_name": "SNOWFLAKE",
                "metric_schema_name": "CORE",
                "metric_name": "DUPLICATE_COUNT",
                "ref_arguments": _ref_arguments("REGION", "TRADE_ID"),
                "schedule": "5 MINUTE",
                "schedule_status": "STARTED",
            }
        )
        changeset = SnowflakeRelation.data_metric_function_changeset(results, model)

        assert changeset is not None
        assert [dmf.arguments for dmf in changeset.to_add] == [("trade_id", "region")]
        assert [dmf.arguments for dmf in changeset.to_drop] == [("REGION", "TRADE_ID")]


class TestParseModelValidation:
    def test_schedule_must_be_a_string(self):
        with pytest.raises(DbtConfigError, match="Unexpected data_metric_schedule"):
            data_metric_schedule(_FakeModel(data_metric_schedule=5))

    def test_blank_schedule_is_none(self):
        assert data_metric_schedule(_FakeModel(data_metric_schedule="   ")) is None

    def test_functions_must_be_a_list(self):
        with pytest.raises(DbtConfigError, match="expected a list"):
            data_metric_functions(_FakeModel(data_metric_functions="null_count"))

    def test_function_entry_requires_metric_and_expression(self):
        with pytest.raises(DbtConfigError, match="requires a non-empty"):
            data_metric_functions(
                _FakeModel(data_metric_functions=[{"metric": "snowflake.core.null_count"}])
            )

    def test_scalar_expression_is_wrapped_in_a_list(self):
        parsed = data_metric_functions(
            _FakeModel(
                data_metric_functions=[
                    {"metric": "snowflake.core.null_count", "expression": "trade_id"}
                ]
            )
        )
        assert parsed == [{"name": "snowflake.core.null_count", "arguments": ["trade_id"]}]
