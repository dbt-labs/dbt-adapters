import json

import agate
import pytest

from dbt_common.clients.agate_helper import table_from_data_flat
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError

from dbt.adapters.snowflake import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDataMetricFunctionAssociation,
    SnowflakeDataMetricsConfig,
)
from dbt.adapters.snowflake.relation_configs.data_metrics import _parse_ref_arguments


REFERENCE_COLUMNS = ["METRIC_DATABASE_NAME", "METRIC_SCHEMA_NAME", "METRIC_NAME", "REF_ARGUMENTS"]
PARAMETER_COLUMNS = ["key", "value", "default", "level", "description", "type"]


class _FakeConfig:
    def __init__(self, values):
        self._values = values

    def __contains__(self, key):
        return key in self._values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _FakeModel:
    """Stands in for a RelationConfig: `.config.get`, plus optional location and columns."""

    def __init__(self, database=None, schema=None, quoting=None, columns=None, **values):
        self.config = _FakeConfig(values)
        self.database = database
        self.schema = schema
        self.quoting_dict = quoting or {}
        self.columns = columns or {}


def _reference(function, *columns, raw_arguments=None):
    database, schema, name = function.split(".")
    arguments = (
        raw_arguments
        if raw_arguments is not None
        else json.dumps([{"domain": "COLUMN", "id": i, "name": c} for i, c in enumerate(columns)])
    )
    return [database, schema, name, arguments]


def _references(*rows):
    return agate.Table([list(row) for row in rows], REFERENCE_COLUMNS)


def _schedule_parameter(value="60 MINUTES", level=""):
    return agate.Table(
        [["DATA_METRIC_SCHEDULE", value, "60 MINUTES", level, "", "STRING"]],
        PARAMETER_COLUMNS,
        # keep '' as a value; agate would otherwise read it as null
        column_types=[agate.Text(cast_nulls=False)] * len(PARAMETER_COLUMNS),
    )


def _results(function_references=None, schedule_parameters=None):
    results = {}
    if function_references is not None:
        results["function_references"] = function_references
    if schedule_parameters is not None:
        results["schedule_parameters"] = schedule_parameters
    return results


def _keys(changes):
    return [association.key for association in changes]


def _data_metrics(**kwargs):
    return SnowflakeDataMetricsConfig.from_relation_config(_FakeModel(**kwargs))


class TestDataMetricsConfigFromModel:
    def test_absent_config_is_not_configured(self):
        config = SnowflakeDataMetricsConfig.from_relation_config(_FakeModel())
        assert config.schedule is None
        assert config.functions is None
        assert not config.is_configured

    def test_explicit_empty_list_is_configured(self):
        config = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(data_metric_functions=[])
        )
        assert config.functions == ()
        assert config.is_configured

    def test_parses_functions_and_columns(self):
        config = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(
                data_metric_functions=[
                    {"function": "snowflake.core.null_count", "columns": ["status"]},
                    {"function": "SNOWFLAKE.CORE.ROW_COUNT", "columns": []},
                    {"function": "a.b.c"},
                ]
            )
        )
        null_count, row_count, no_columns = config.functions
        assert null_count.function_sql == "snowflake.core.null_count"
        assert null_count.columns_sql == "status"
        assert null_count.key == (("SNOWFLAKE", "CORE", "NULL_COUNT"), ("STATUS",))
        assert row_count.columns_sql == ""
        assert row_count.key == (("SNOWFLAKE", "CORE", "ROW_COUNT"), ())
        assert no_columns.columns == ()

    def test_quoted_identifiers_keep_case(self):
        (association,) = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(
                data_metric_functions=[
                    {"function": 'my_db."My Schema"."My.Dmf"', "columns": ['"Order Id"', "b"]}
                ]
            )
        ).functions
        assert association.key == (("MY_DB", "My Schema", "My.Dmf"), ("Order Id", "B"))
        assert association.function_sql == 'my_db."My Schema"."My.Dmf"'
        assert association.columns_sql == '"Order Id", b'

    def test_function_name_supports_quoted_dots_and_escaped_quotes(self):
        (association,) = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(
                data_metric_functions=[{"function": ' "my.db" . "My ""Schema""" . "My.Dmf" '}]
            )
        ).functions
        assert association.key == (("my.db", 'My "Schema"', "My.Dmf"), ())
        assert association.function_sql == '"my.db"."My ""Schema"""."My.Dmf"'

    @pytest.mark.parametrize(
        "column",
        [
            '"Order "Id"',
            '"amount',
            'amount"',
            '"',
            'amo"unt',
            'amount""',
            '"amount"suffix',
            '"amount"."other"',
            '"amount""',
        ],
    )
    def test_malformed_identifier_quotes_are_rejected(self, column):
        with pytest.raises(DbtConfigError, match="Invalid.*identifier") as exc:
            _data_metrics(
                data_metric_functions=[
                    {"function": "snowflake.core.null_count", "columns": [column]}
                ]
            )
        assert "data_metric_functions[0]" in str(exc.value)
        assert repr(column) in str(exc.value)

    @pytest.mark.parametrize(
        "column,sql,normalized",
        [
            ('"Order ""Id"""', '"Order ""Id"""', 'Order "Id"'),
            ('"a""""b"', '"a""""b"', 'a""b'),
            ('  "Order Id"  ', '"Order Id"', "Order Id"),
            (" amount ", "amount", "AMOUNT"),
        ],
    )
    def test_valid_identifier_quotes_and_normalization(self, column, sql, normalized):
        (association,) = _data_metrics(
            data_metric_functions=[{"function": "snowflake.core.null_count", "columns": [column]}]
        ).functions
        assert association.columns_sql == sql
        assert association.key[1] == (normalized,)

    @pytest.mark.parametrize(
        "entry,message",
        [
            ("snowflake.core.null_count", "expected a mapping"),
            ({"columns": ["a"]}, "missing the required `function`"),
            ({"function": "null_count", "columns": ["a"]}, "must be fully qualified"),
            ({"function": "core.null_count", "columns": ["a"]}, "must be fully qualified"),
            ({"function": "snowflake.core.null_count", "columns": "a"}, "expected a list"),
            ({"function": "snowflake..null_count", "columns": ["a"]}, "must be fully qualified"),
            ({"function": 'snowflake.core."open', "columns": []}, "must be fully qualified"),
            ({"function": "snowflake.core.null_count", "columns": [1]}, "identifier string"),
            (
                {"function": "snowflake.core.null_count", "columns": ["a"], "expectation": "x"},
                "expectations are not supported",
            ),
        ],
    )
    def test_invalid_entries_fail_clearly(self, entry, message):
        with pytest.raises(DbtConfigError, match=message):
            SnowflakeDataMetricsConfig.from_relation_config(
                _FakeModel(data_metric_functions=[entry])
            )

    def test_names_are_passed_through_unvalidated(self):
        # Like grantees, names are not validated; Snowflake rejects invalid ones.
        (association,) = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(
                data_metric_functions=[
                    {"function": "snowflake.core.null-count", "columns": ["a; drop table x", "b)"]}
                ]
            )
        ).functions
        assert association.function_sql == "snowflake.core.null-count"
        assert association.columns_sql == "a; drop table x, b)"

    @pytest.mark.parametrize("function", ["count_positive", '"Count Positive"', "core.null_count"])
    def test_partial_names_are_rejected_even_with_model_schema(self, function):
        with pytest.raises(DbtConfigError, match="must be fully qualified"):
            SnowflakeDataMetricsConfig.from_relation_config(
                _FakeModel(
                    database="MY_DB",
                    schema="MY_SCHEMA",
                    data_metric_functions=[{"function": function}],
                )
            )

    def test_functions_must_be_a_list(self):
        with pytest.raises(DbtConfigError, match="expected a list"):
            SnowflakeDataMetricsConfig.from_relation_config(
                _FakeModel(data_metric_functions={"function": "snowflake.core.row_count"})
            )

    def test_duplicate_associations_are_deduplicated(self):
        config = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(
                data_metric_functions=[
                    {"function": "snowflake.core.null_count", "columns": ["a"]},
                    {"function": "SNOWFLAKE.CORE.NULL_COUNT", "columns": ["A"]},
                ]
            )
        )
        assert config.functions is not None
        assert len(config.functions) == 1
        assert config.functions[0].function_sql == "snowflake.core.null_count"

    def test_multiple_columns_preserve_argument_order(self):
        config = _data_metrics(
            data_metric_functions=[
                {"function": "my_db.my_schema.compare_values", "columns": ["amount", "limit"]},
                {"function": "my_db.my_schema.compare_values", "columns": ["limit", "amount"]},
            ]
        )
        first, second = config.functions
        assert first.columns_sql == "amount, limit"
        assert second.columns_sql == "limit, amount"
        assert first.key != second.key

    def test_same_function_can_bind_different_columns(self):
        config = _data_metrics(
            data_metric_functions=[
                {"function": "snowflake.core.null_count", "columns": ["status"]},
                {"function": "snowflake.core.null_count", "columns": ["amount"]},
            ]
        )
        assert [a.key for a in config.functions] == [
            (("SNOWFLAKE", "CORE", "NULL_COUNT"), ("STATUS",)),
            (("SNOWFLAKE", "CORE", "NULL_COUNT"), ("AMOUNT",)),
        ]


class TestDataMetricSchedule:
    @pytest.mark.parametrize(
        "schedule",
        [
            "5 MINUTE",
            "USING CRON 0 6 * * * UTC",
            "TRIGGER_ON_CHANGES",
            "",  # pauses all DMFs on the object
            "5 HOURS",  # invalid; Snowflake rejects it at ALTER time
        ],
    )
    def test_schedule_is_passed_through(self, schedule):
        config = SnowflakeDataMetricsConfig.from_relation_config(
            _FakeModel(data_metric_schedule=schedule)
        )
        assert config.schedule == schedule

    def test_schedule_must_be_a_string(self):
        with pytest.raises(DbtConfigError, match="expected a string"):
            SnowflakeDataMetricsConfig.from_relation_config(_FakeModel(data_metric_schedule=5))

    def test_explicit_null_schedule_is_invalid(self):
        with pytest.raises(DbtConfigError, match="null is not allowed"):
            SnowflakeDataMetricsConfig.from_relation_config(_FakeModel(data_metric_schedule=None))

    def test_normalization(self):
        assert SnowflakeDataMetricsConfig(schedule="5 minutes").schedule_normalized == "5 MINUTE"
        assert SnowflakeDataMetricsConfig(schedule="05  MINUTE").schedule_normalized == "5 MINUTE"
        assert (
            SnowflakeDataMetricsConfig(schedule="using cron 0 6 * * * utc").schedule_normalized
            == "USING CRON 0 6 * * * UTC"
        )
        assert SnowflakeDataMetricsConfig(schedule="").schedule_normalized == ""
        assert SnowflakeDataMetricsConfig().schedule_normalized is None


class TestDataMetricsConfigFromResults:
    def test_no_attached_functions_is_empty_not_unknown(self):
        config = SnowflakeDataMetricsConfig.from_relation_results(_results(_references()))
        assert config.functions == ()

    def test_unread_halves_stay_none(self):
        config = SnowflakeDataMetricsConfig.from_relation_results(_results())
        assert config.functions is None
        assert config.schedule is None

    def test_parses_references(self):
        config = SnowflakeDataMetricsConfig.from_relation_results(
            _results(
                _references(
                    _reference("SNOWFLAKE.CORE.NULL_COUNT", "STATUS"),
                    _reference("SNOWFLAKE.CORE.ROW_COUNT"),
                    _reference("MY_DB.MY_SCHEMA.PAIR_CHECK", "Order Id", "B"),
                )
            )
        )
        null_count, row_count, pair_check = config.functions
        assert null_count.key == (("SNOWFLAKE", "CORE", "NULL_COUNT"), ("STATUS",))
        # metadata names are rendered quoted so DROP hits the exact stored name
        assert null_count.function_sql == '"SNOWFLAKE"."CORE"."NULL_COUNT"'
        assert null_count.columns_sql == '"STATUS"'
        assert row_count.columns == ()
        assert row_count.columns_sql == ""
        assert pair_check.columns_sql == '"Order Id", "B"'

    def test_ref_arguments_may_already_be_a_list(self):
        assert _parse_ref_arguments([{"domain": "COLUMN", "name": "STATUS"}]) == ["STATUS"]

    @pytest.mark.parametrize(
        "raw_arguments",
        [
            "not json",
            json.dumps({"name": "STATUS"}),
            json.dumps(["STATUS"]),
            json.dumps([{"domain": "COLUMN"}]),
            json.dumps([{"name": "STATUS"}]),
            json.dumps([{"domain": "TABLE", "name": "OTHER"}]),
        ],
    )
    def test_unparseable_ref_arguments_fail(self, raw_arguments):
        with pytest.raises(DbtRuntimeError):
            SnowflakeDataMetricsConfig.from_relation_results(
                _results(
                    _references(
                        _reference("SNOWFLAKE.CORE.NULL_COUNT", raw_arguments=raw_arguments)
                    )
                )
            )

    def test_null_ref_arguments_fail(self):
        with pytest.raises(DbtRuntimeError):
            SnowflakeDataMetricsConfig.from_relation_results(
                _results(_references(["SNOWFLAKE", "CORE", "NULL_COUNT", None]))
            )

    def test_missing_reference_names_fail(self):
        table = agate.Table(
            [["SNOWFLAKE", "CORE"]], ["METRIC_DATABASE_NAME", "METRIC_SCHEMA_NAME"]
        )
        with pytest.raises(DbtRuntimeError, match="unexpected metric name"):
            SnowflakeDataMetricsConfig.from_relation_results(_results(table))

    @pytest.mark.parametrize(
        "schedule", ["5 MINUTE", "60 MINUTES", "USING CRON 0 6 * * * UTC", "TRIGGER_ON_CHANGES"]
    )
    def test_schedule_readback_uses_effective_value(self, schedule):
        parameters = table_from_data_flat([{"value": schedule}], ["value"])
        config = SnowflakeDataMetricsConfig.from_relation_results(
            _results(schedule_parameters=parameters)
        )
        assert isinstance(parameters.rows[0]["value"], str)
        assert config.schedule == schedule

    def test_non_string_schedule_from_metadata_fails(self):
        parameters = table_from_data_flat([{"value": 5}], ["value"])
        with pytest.raises(DbtRuntimeError, match="Could not parse DATA_METRIC_SCHEDULE"):
            SnowflakeDataMetricsConfig.from_relation_results(
                _results(schedule_parameters=parameters)
            )

    def test_empty_schedule_on_object_is_kept(self):
        config = SnowflakeDataMetricsConfig.from_relation_results(
            _results(schedule_parameters=_schedule_parameter("", "TABLE"))
        )
        assert config.schedule == ""

    def test_default_schedule_is_read(self):
        config = SnowflakeDataMetricsConfig.from_relation_results(
            _results(schedule_parameters=_schedule_parameter("60 MINUTES", ""))
        )
        assert config.schedule == "60 MINUTES"


class TestDataMetricsChangeset:
    def test_absent_config_with_attached_functions_is_untouched(self):
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            _results(),
            _data_metrics(),  # nothing read, nothing managed
        )
        assert changeset is None

    def test_adds_everything_on_fresh_relation(self):
        data_metrics = _data_metrics(
            data_metric_schedule="5 MINUTE",
            data_metric_functions=[
                {"function": "snowflake.core.null_count", "columns": ["status"]},
                {"function": "snowflake.core.row_count", "columns": []},
            ],
        )
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            _results(_references(), _schedule_parameter()), data_metrics
        )
        assert changeset.schedule == "5 MINUTE"
        assert changeset.drops == []
        assert _keys(changeset.adds) == [
            (("SNOWFLAKE", "CORE", "NULL_COUNT"), ("STATUS",)),
            (("SNOWFLAKE", "CORE", "ROW_COUNT"), ()),
        ]

    def test_matching_state_is_idempotent(self):
        data_metrics = _data_metrics(
            data_metric_schedule="5 minutes",
            data_metric_functions=[
                {"function": "snowflake.core.null_count", "columns": ["status"]},
                {"function": "snowflake.core.row_count"},
            ],
        )
        results = _results(
            _references(
                _reference("SNOWFLAKE.CORE.ROW_COUNT"),
                _reference("SNOWFLAKE.CORE.NULL_COUNT", "STATUS"),
            ),
            _schedule_parameter("5 MINUTE", "TABLE"),
        )
        assert SnowflakeRelation.data_metrics_config_changeset(results, data_metrics) is None

    def test_replacement_and_removal(self):
        data_metrics = _data_metrics(
            data_metric_functions=[
                {"function": "snowflake.core.null_count", "columns": ["amount"]},
            ],
        )
        results = _results(
            _references(
                _reference("SNOWFLAKE.CORE.NULL_COUNT", "STATUS"),
                _reference("SNOWFLAKE.CORE.ROW_COUNT"),
            )
        )
        changeset = SnowflakeRelation.data_metrics_config_changeset(results, data_metrics)
        assert changeset.schedule is None
        assert _keys(changeset.drops) == [
            (("SNOWFLAKE", "CORE", "NULL_COUNT"), ("STATUS",)),
            (("SNOWFLAKE", "CORE", "ROW_COUNT"), ()),
        ]
        assert _keys(changeset.adds) == [(("SNOWFLAKE", "CORE", "NULL_COUNT"), ("AMOUNT",))]

    def test_empty_list_drops_all(self):
        results = _results(
            _references(
                _reference("SNOWFLAKE.CORE.NULL_COUNT", "STATUS"),
                _reference("SNOWFLAKE.CORE.ROW_COUNT"),
            )
        )
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            results, _data_metrics(data_metric_functions=[])
        )
        assert len(changeset.drops) == 2
        assert changeset.adds == []

    def test_empty_list_on_empty_relation_is_noop(self):
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            _results(_references()), _data_metrics(data_metric_functions=[])
        )
        assert changeset is None

    def test_column_order_is_significant(self):
        data_metrics = _data_metrics(
            data_metric_functions=[
                {"function": "my_db.my_schema.pair_check", "columns": ["a", "b"]}
            ]
        )
        results = _results(_references(_reference("MY_DB.MY_SCHEMA.PAIR_CHECK", "B", "A")))
        changeset = SnowflakeRelation.data_metrics_config_changeset(results, data_metrics)
        assert _keys(changeset.drops) == [(("MY_DB", "MY_SCHEMA", "PAIR_CHECK"), ("B", "A"))]
        assert _keys(changeset.adds) == [(("MY_DB", "MY_SCHEMA", "PAIR_CHECK"), ("A", "B"))]

    def test_quoted_column_does_not_match_folded_name(self):
        data_metrics = _data_metrics(
            data_metric_functions=[
                {"function": "snowflake.core.null_count", "columns": ['"status"']}
            ]
        )
        results = _results(_references(_reference("SNOWFLAKE.CORE.NULL_COUNT", "STATUS")))
        changeset = SnowflakeRelation.data_metrics_config_changeset(results, data_metrics)
        assert len(changeset.drops) == 1
        assert len(changeset.adds) == 1

    def test_schedule_only_leaves_associations_alone(self):
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            _results(schedule_parameters=_schedule_parameter("5 MINUTE", "TABLE")),
            _data_metrics(data_metric_schedule="USING CRON 0 6 * * * UTC"),
        )
        assert changeset.schedule == "USING CRON 0 6 * * * UTC"
        assert changeset.adds == []
        assert changeset.drops == []

    def test_default_schedule_equal_to_desired_is_unchanged(self):
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            _results(schedule_parameters=_schedule_parameter("60 MINUTES", "")),
            _data_metrics(data_metric_schedule="60 MINUTE"),
        )
        assert changeset is None

    def test_default_schedule_different_from_desired_is_changed(self):
        changeset = SnowflakeRelation.data_metrics_config_changeset(
            _results(schedule_parameters=_schedule_parameter("60 MINUTES", "")),
            _data_metrics(data_metric_schedule="5 MINUTE"),
        )
        assert changeset.schedule == "5 MINUTE"

    def test_functions_configured_but_not_read_fails(self):
        with pytest.raises(DbtRuntimeError, match="were not read"):
            SnowflakeRelation.data_metrics_config_changeset(
                _results(),
                _data_metrics(
                    data_metric_functions=[{"function": "snowflake.core.row_count"}],
                ),
            )

    def test_association_is_hashable(self):
        association = SnowflakeDataMetricFunctionAssociation.from_config_entry(
            {"function": "snowflake.core.row_count"}, "data_metric_functions[0]"
        )
        assert hash(association)
