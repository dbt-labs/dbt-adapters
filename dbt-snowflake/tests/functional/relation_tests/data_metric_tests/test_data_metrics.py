import json
from typing import List, Optional, Tuple

import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.data_metric_tests import models
from tests.functional.utils import update_model
from dbt.tests.util import write_file


NULL_COUNT_STATUS = ("NULL_COUNT", ("STATUS",))
NULL_COUNT_AMOUNT = ("NULL_COUNT", ("AMOUNT",))
ROW_COUNT = ("ROW_COUNT", ())


def attached(project, name: str, domain: str = "table") -> List[Tuple[str, Tuple[str, ...]]]:
    sql = f"""
    select metric_name, ref_arguments
    from table({project.database}.information_schema.data_metric_function_references(
        ref_entity_name => '{project.database}.{project.test_schema}.{name}',
        ref_entity_domain => '{domain}'
    ))
    where metric_database_name = 'SNOWFLAKE' and metric_schema_name = 'CORE'
    """
    rows = project.run_sql(sql, fetch="all")
    return sorted(
        (metric_name, tuple(argument["name"] for argument in json.loads(arguments)))
        for metric_name, arguments in rows
    )


def schedule(project, name: str, kind: str = "table") -> Tuple[Optional[str], str]:
    rows = project.run_sql(
        f"show parameters like 'DATA_METRIC_SCHEDULE' in table "
        f"{project.database}.{project.test_schema}.{name}",
        fetch="all",
    )
    # key, value, default, level, ...
    return rows[0][1], rows[0][3]


def add_manually(project, name: str, function: str, columns: str, kind: str = "table"):
    project.run_sql(
        f"alter {kind} {project.database}.{project.test_schema}.{name} "
        f"add data metric function {function} on ({columns})"
    )


def assert_no_dmf_ddl(logs: str):
    assert_message_in_logs("set data_metric_schedule", logs, expected_pass=False)
    assert_message_in_logs("add data metric function", logs, expected_pass=False)
    assert_message_in_logs("drop data metric function", logs, expected_pass=False)


class DataMetricsBase:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")


class TestDataMetricsTable(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.TABLE}

    def test_attach_rebuild_and_replace(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("set data_metric_schedule = '5 MINUTE'", logs)
        assert_message_in_logs("snowflake.core.row_count on ()", logs)
        assert attached(project, "my_model") == [NULL_COUNT_STATUS, ROW_COUNT]
        assert schedule(project, "my_model") == ("5 MINUTE", "TABLE")

        # CREATE OR REPLACE removes associations; the rerun must restore them.
        run_dbt(["run"])
        assert attached(project, "my_model") == [NULL_COUNT_STATUS, ROW_COUNT]
        assert schedule(project, "my_model") == ("5 MINUTE", "TABLE")

        update_model(project, "my_model", models.TABLE_REPLACED)
        run_dbt(["run"])
        assert attached(project, "my_model") == [NULL_COUNT_AMOUNT]

        update_model(project, "my_model", models.TABLE_INVALID_CONFIG)
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert "Invalid schedule was specified" in logs

    @pytest.mark.parametrize("value", ["O'Hare", "two '' quotes", "backslash\\'quote"])
    def test_schedule_quotes_are_rejected_as_invalid_values_not_sql(self, project, value):
        update_model(
            project, "my_model", models.model("table", f", data_metric_schedule={value!r}")
        )
        results, _ = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results.results) == 1
        assert results.results[0].status == "error"
        assert "Invalid schedule was specified" in results.results[0].message


class TestDataMetricsView(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.VIEW}

    def test_attach_and_rebuild(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("alter view", logs)
        assert attached(project, "my_model", "view") == [NULL_COUNT_STATUS, ROW_COUNT]
        assert schedule(project, "my_model", "view")[0] == "5 MINUTE"

        run_dbt(["run"])
        assert attached(project, "my_model", "view") == [NULL_COUNT_STATUS, ROW_COUNT]


class TestDataMetricsIncremental(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.INCREMENTAL}

    def test_diff_full_refresh_and_empty_list(self, project):
        run_dbt(["run"])
        assert attached(project, "my_model") == [NULL_COUNT_STATUS, ROW_COUNT]

        # Non-full-refresh run keeps the table, so nothing needs to change.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("data metric functions already in sync", logs)
        assert_no_dmf_ddl(logs)
        assert attached(project, "my_model") == [NULL_COUNT_STATUS, ROW_COUNT]

        run_dbt(["run", "--full-refresh"])
        assert attached(project, "my_model") == [NULL_COUNT_STATUS, ROW_COUNT]
        assert schedule(project, "my_model") == ("5 MINUTE", "TABLE")

        # Only the removed association is dropped; ROW_COUNT is not re-added.
        update_model(project, "my_model", models.INCREMENTAL_ROW_COUNT_ONLY)
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("drop data metric function", logs)
        assert_message_in_logs("add data metric function", logs, expected_pass=False)
        assert attached(project, "my_model") == [ROW_COUNT]
        assert schedule(project, "my_model") == ("5 MINUTE", "TABLE")

        update_model(project, "my_model", models.INCREMENTAL_EMPTY_LIST)
        run_dbt(["run"])
        assert attached(project, "my_model") == []

        # An explicit empty list also removes associations dbt did not add.
        add_manually(project, "my_model", "snowflake.core.null_count", "status")
        run_dbt(["run"])
        assert attached(project, "my_model") == []


class TestDataMetricsDynamicTable(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.DYNAMIC_TABLE}

    def test_attach_and_reconcile(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("alter dynamic table", logs)
        assert attached(project, "my_model") == [NULL_COUNT_STATUS, ROW_COUNT]
        assert schedule(project, "my_model") == ("5 MINUTE", "TABLE")

        # Unchanged config: the build is skipped and nothing needs to change.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("data metric functions already in sync", logs)
        assert_no_dmf_ddl(logs)

        # A DMF-only config change still applies even though the build is skipped.
        update_model(project, "my_model", models.DYNAMIC_TABLE_ROW_COUNT_ONLY)
        run_dbt(["run"])
        assert attached(project, "my_model") == [ROW_COUNT]

        run_dbt(["run", "--full-refresh"])
        assert attached(project, "my_model") == [ROW_COUNT]


def attached_all(project, name: str) -> List[Tuple[str, str, Tuple[str, ...]]]:
    rows = project.run_sql(
        "select metric_schema_name, metric_name, ref_arguments from table("
        f"{project.database}.information_schema.data_metric_function_references("
        f"ref_entity_name => '{project.database}.{project.test_schema}.{name}', "
        "ref_entity_domain => 'table'))",
        fetch="all",
    )
    return sorted(
        (schema, metric, tuple(argument["name"] for argument in json.loads(arguments)))
        for schema, metric, arguments in rows
    )


class TestDataMetricsModelYaml(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.YAML_MODEL, "schema.yml": models.MODEL_SCHEMA_YML}

    def test_model_yaml_dmfs_and_existing_custom_function(self, project):
        # The binding config uses a fully qualified name for this pre-existing DMF.
        project.run_sql(
            f"create or replace data metric function "
            f"{project.database}.{project.test_schema}.local_null_count("
            "arg_t table(arg_c varchar)) returns number as "
            "'select count_if(arg_c is null) from arg_t'"
        )
        run_dbt(["run"])
        schema = project.test_schema.upper()
        assert attached_all(project, "my_model") == [
            ("CORE", "NULL_COUNT", ("AMOUNT",)),
            ("CORE", "NULL_COUNT", ("STATUS",)),
            ("CORE", "ROW_COUNT", ()),
            (schema, "LOCAL_NULL_COUNT", ("STATUS",)),
        ]

        # Unchanged config on an incremental run: names match, so nothing changes.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_message_in_logs("data metric functions already in sync", logs)

        # Removing entries from the model's desired set drops those associations.
        write_file(models.MODEL_SCHEMA_YML_REMOVED, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        assert attached_all(project, "my_model") == [("CORE", "NULL_COUNT", ("STATUS",))]


class TestDataMetricsMultipleColumns(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": """
{{ config(
    materialized='incremental',
    data_metric_functions=[{
        'function': target.database ~ '.' ~ target.schema ~ '.invalid_order_count',
        'columns': ['status', 'amount'],
    }],
) }}
select id, status, amount::number as amount from {{ ref('my_seed') }}
"""
        }

    def test_ordered_columns_and_idempotent_rerun(self, project):
        project.run_sql(
            f"create or replace data metric function "
            f"{project.database}.{project.test_schema}.invalid_order_count("
            "arg_t table(arg_status varchar, arg_amount number)) returns number as "
            "'select count_if(arg_status is null and arg_amount < 0) from arg_t'"
        )
        run_dbt(["run"])
        expected = [(project.test_schema.upper(), "INVALID_ORDER_COUNT", ("STATUS", "AMOUNT"))]
        assert attached_all(project, "my_model") == expected

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_no_dmf_ddl(logs)
        assert attached_all(project, "my_model") == expected


class TestDataMetricsMissingFunction(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.YAML_MODEL, "schema.yml": models.MODEL_SCHEMA_YML}

    def test_missing_function_fails_and_can_be_retried(self, project):
        # A missing DMF must fail the model instead of silently leaving it unbound.
        results, _ = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results.results) == 1
        result = results.results[0]
        assert result.status == "error"
        assert "LOCAL_NULL_COUNT" in result.message.upper()
        schema = project.test_schema.upper()
        assert not [a for a in attached_all(project, "my_model") if a[0] == schema]

        # Once it exists, the next run attaches it.
        project.run_sql(
            f"create or replace data metric function "
            f"{project.database}.{project.test_schema}.local_null_count("
            "arg_t table(arg_c varchar)) returns number as "
            "'select count_if(arg_c is null) from arg_t'"
        )
        run_dbt(["run"])
        assert (schema, "LOCAL_NULL_COUNT", ("STATUS",)) in attached_all(project, "my_model")


class TestDataMetricsAbsentConfig(DataMetricsBase):
    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_model.sql": models.INCREMENTAL_NO_CONFIG}

    def test_manual_state_is_preserved(self, project):
        run_dbt(["run"])
        add_manually(project, "my_model", "snowflake.core.null_count", "status")
        project.run_sql(
            f"alter table {project.database}.{project.test_schema}.my_model "
            "set data_metric_schedule = '5 MINUTE'"
        )

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert_no_dmf_ddl(logs)
        assert attached(project, "my_model") == [NULL_COUNT_STATUS]
        assert schedule(project, "my_model") == ("5 MINUTE", "TABLE")
