"""
Functional tests for custom incremental dynamic tables (CI DT): a `dynamic_table` model with
`refresh_mode='CUSTOM_INCREMENTAL'` and `incremental_strategy='merge'` deploys as
`create dynamic table X (<columns>) ... refresh using (merge into SELF ...)`, reading its change
source with `{{ changes(...) }}`. These require a live Snowflake connection.

Test flow:
  1. ARRANGE -- the `setup` fixture creates the change source `orders` with change tracking on
     and builds the models (`dbt run`).
  2. ACT -- DML against `orders`, then `alter dynamic table ... refresh`; or edit a model and run.
  3. ASSERT -- read the CI DT's rows back, and its definition with `describe_dynamic_table`.
  4. TEARDOWN -- the `setup` fixture drops the schema so each test starts clean.
"""

import os

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.utils import describe_dynamic_table, update_model

WAREHOUSE = os.getenv("SNOWFLAKE_TEST_WAREHOUSE")

SOURCES = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: orders
"""


def _ci_model(
    *,
    unique_key="'order_id'",
    delete_condition="'DBT_INTERNAL_SOURCE.is_deleted'",
    mode="default",
    extra_config="",
    extra_columns="",
):
    delete_config = f"delete_condition={delete_condition}," if delete_condition else ""
    return f"""
{{{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='{WAREHOUSE}',
    refresh_mode='CUSTOM_INCREMENTAL',
    incremental_strategy='merge',
    unique_key={unique_key},
    {delete_config}
    scheduler='disable',
    {extra_config}
) }}}}
select
    o.order_id,
    o.amount,
    o.METADATA$ACTION = 'DELETE' and not o.METADATA$ISUPDATE as is_deleted{extra_columns}
from {{{{ changes(source('raw', 'orders'), '{mode}') }}}} as o
-- an UPDATE is a DELETE + INSERT pair in CHANGES; keep one row per key
where not (o.METADATA$ACTION = 'DELETE' and o.METADATA$ISUPDATE)
"""


CI_MODEL = _ci_model()

REGULAR_MODEL = f"""
{{{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='{WAREHOUSE}',
    scheduler='disable',
) }}}}
select order_id, amount, false as is_deleted from {{{{ source('raw', 'orders') }}}}
"""


def _rows(project, name):
    rows = project.run_sql(
        f"select order_id, amount, is_deleted from {project.database}.{project.test_schema}.{name}"
        " order by order_id, amount",
        fetch="all",
    )
    return [(int(order_id), float(amount), is_deleted) for order_id, amount, is_deleted in rows]


def _created_on(project, name):
    rows = project.run_sql(
        f"show dynamic tables like '{name.upper()}' in schema"
        f" {project.database}.{project.test_schema}",
        fetch="all",
    )
    return next(row[0] for row in rows if str(row[1]).upper() == name.upper())


def _orders(project):
    return f"{project.database}.{project.test_schema}.orders"


def refresh_dynamic_table(project, name):
    project.run_sql(f"alter dynamic table {project.database}.{project.test_schema}.{name} refresh")


def _create_orders(project):
    project.run_sql(f"create schema if not exists {project.database}.{project.test_schema}")
    project.run_sql(
        f"create or replace table {_orders(project)} (order_id number, amount number(10,2))"
        " change_tracking = true"
    )


class _CustomIncrementalBase:
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"sources.yml": SOURCES, "orders_ci.sql": CI_MODEL}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        _create_orders(project)
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")


class TestCustomIncrementalMerge(_CustomIncrementalBase):
    def test_creates_a_refresh_using_merge_definition(self, project):
        dynamic_table = describe_dynamic_table(project, "orders_ci")
        assert dynamic_table.refresh_mode == "CUSTOM_INCREMENTAL"
        query = dynamic_table.query.lower()
        assert "refresh using (" in query
        assert "merge into self as dbt_internal_dest" in query
        assert "changes(information => default)" in query
        assert "at(offset => 0)" not in query

    def test_merge_applies_inserts_updates_and_deletes(self, project):
        project.run_sql(f"insert into {_orders(project)} values (1, 10), (2, 20), (3, 30)")
        refresh_dynamic_table(project, "orders_ci")
        assert _rows(project, "orders_ci") == [
            (1, 10.0, False),
            (2, 20.0, False),
            (3, 30.0, False),
        ]

        project.run_sql(f"update {_orders(project)} set amount = 21 where order_id = 2")
        project.run_sql(f"delete from {_orders(project)} where order_id = 3")
        project.run_sql(f"insert into {_orders(project)} values (4, 40)")
        refresh_dynamic_table(project, "orders_ci")
        assert _rows(project, "orders_ci") == [
            (1, 10.0, False),
            (2, 21.0, False),
            (4, 40.0, False),
        ]

        # a deleted key can come back
        project.run_sql(f"insert into {_orders(project)} values (3, 33)")
        refresh_dynamic_table(project, "orders_ci")
        assert _rows(project, "orders_ci") == [
            (1, 10.0, False),
            (2, 21.0, False),
            (3, 33.0, False),
            (4, 40.0, False),
        ]

    def test_unchanged_model_is_a_no_op(self, project):
        created_on = _created_on(project, "orders_ci")
        _, logs = run_dbt_and_capture(["run"])
        assert "No configuration changes were identified" in logs
        assert _created_on(project, "orders_ci") == created_on

    def test_full_refresh_rebuilds(self, project):
        project.run_sql(f"insert into {_orders(project)} values (1, 10)")
        created_on = _created_on(project, "orders_ci")
        run_dbt(["run", "--full-refresh"])
        assert _created_on(project, "orders_ci") != created_on
        assert describe_dynamic_table(project, "orders_ci").refresh_mode == "CUSTOM_INCREMENTAL"


class TestCustomIncrementalAlter(_CustomIncrementalBase):
    """With the flag off, a config change an ALTER can apply is altered, as on a regular DT."""

    def test_target_lag_change_is_altered_in_place(self, project):
        project.run_sql(f"insert into {_orders(project)} values (1, 10)")
        refresh_dynamic_table(project, "orders_ci")
        created_on = _created_on(project, "orders_ci")

        update_model(
            project,
            "orders_ci",
            _ci_model(extra_config="target_lag='2 minutes',").replace("scheduler='disable',", ""),
        )
        run_dbt(["run"])
        dynamic_table = describe_dynamic_table(project, "orders_ci")
        assert dynamic_table.target_lag == "2 minutes"
        assert dynamic_table.refresh_mode == "CUSTOM_INCREMENTAL"
        assert _created_on(project, "orders_ci") == created_on
        assert _rows(project, "orders_ci") == [(1, 10.0, False)]


class TestCustomIncrementalCompositeKeyAppendOnly(_CustomIncrementalBase):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "sources.yml": SOURCES,
            "orders_ci.sql": _ci_model(
                unique_key="['order_id', 'amount']",
                delete_condition=None,
                mode="append_only",
                extra_config="transient=true,",
            ),
        }

    def test_append_only_ignores_deletes(self, project):
        assert "changes(information => append_only)" in (
            describe_dynamic_table(project, "orders_ci").query.lower()
        )
        project.run_sql(f"insert into {_orders(project)} values (1, 10), (2, 20)")
        refresh_dynamic_table(project, "orders_ci")
        project.run_sql(f"delete from {_orders(project)} where order_id = 1")
        project.run_sql(f"insert into {_orders(project)} values (1, 11)")
        refresh_dynamic_table(project, "orders_ci")
        assert _rows(project, "orders_ci") == [
            (1, 10.0, False),
            (1, 11.0, False),
            (2, 20.0, False),
        ]


class TestCustomIncrementalEphemeralUpstream(_CustomIncrementalBase):
    """Discovery runs the compiled SQL, so an ephemeral model's injected CTE comes along."""

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "sources.yml": SOURCES,
            "multiplier.sql": "{{ config(materialized='ephemeral') }} select 2 as factor",
            "orders_ci.sql": _ci_model(
                extra_columns=",\n    o.amount * (select factor from {{ ref('multiplier') }}) as doubled"
            ),
        }

    def test_ephemeral_upstream_builds_and_refreshes(self, project):
        project.run_sql(f"insert into {_orders(project)} values (1, 10)")
        refresh_dynamic_table(project, "orders_ci")
        doubled = project.run_sql(
            f"select doubled from {project.database}.{project.test_schema}.orders_ci",
            fetch="one",
        )[0]
        assert float(doubled) == 20.0


class TestCustomIncrementalEmpty(_CustomIncrementalBase):
    def test_empty_run_stays_empty_until_full_refresh(self, project):
        run_dbt(["run", "--empty", "--full-refresh"])
        assert "limit 0" in describe_dynamic_table(project, "orders_ci").query.lower()
        project.run_sql(f"insert into {_orders(project)} values (1, 10)")
        refresh_dynamic_table(project, "orders_ci")
        assert _rows(project, "orders_ci") == []

        run_dbt(["run", "--full-refresh"])
        project.run_sql(f"insert into {_orders(project)} values (2, 20)")
        refresh_dynamic_table(project, "orders_ci")
        assert (2, 20.0, False) in _rows(project, "orders_ci")


class TestCustomIncrementalSwitch(_CustomIncrementalBase):
    """CREATE OR ALTER can't convert between a regular and a CI DT, so a switch replaces."""

    def test_switch_to_regular_and_back(self, project):
        project.run_sql(f"insert into {_orders(project)} values (1, 10)")

        update_model(project, "orders_ci", REGULAR_MODEL)
        run_dbt(["run"])
        assert describe_dynamic_table(project, "orders_ci").refresh_mode != "CUSTOM_INCREMENTAL"
        assert _rows(project, "orders_ci") == [(1, 10.0, False)]

        update_model(project, "orders_ci", CI_MODEL)
        run_dbt(["run"])
        assert describe_dynamic_table(project, "orders_ci").refresh_mode == "CUSTOM_INCREMENTAL"


class TestCustomIncrementalSwitchContinue(_CustomIncrementalBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "continue"}}

    def test_continue_skips_the_switch(self, project):
        update_model(project, "orders_ci", REGULAR_MODEL)
        _, logs = run_dbt_and_capture(["run"])
        assert "`on_configuration_change` was set to `continue`" in logs
        assert describe_dynamic_table(project, "orders_ci").refresh_mode == "CUSTOM_INCREMENTAL"


class TestCustomIncrementalSwitchFail(_CustomIncrementalBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "fail"}}

    def test_fail_raises_on_the_switch(self, project):
        update_model(project, "orders_ci", REGULAR_MODEL)
        run_dbt(["run"], expect_pass=False)
        assert describe_dynamic_table(project, "orders_ci").refresh_mode == "CUSTOM_INCREMENTAL"


class TestCustomIncrementalCreateOrAlter(_CustomIncrementalBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    def test_body_edit_applies_in_place_to_later_changes(self, project):
        project.run_sql(f"insert into {_orders(project)} values (1, 10)")
        refresh_dynamic_table(project, "orders_ci")
        created_on = _created_on(project, "orders_ci")

        update_model(
            project, "orders_ci", _ci_model(extra_columns=",\n    o.amount * 2 as doubled")
        )
        run_dbt(["run"])
        assert _created_on(project, "orders_ci") == created_on
        assert "doubled" in describe_dynamic_table(project, "orders_ci").query.lower()

        project.run_sql(f"insert into {_orders(project)} values (2, 20)")
        refresh_dynamic_table(project, "orders_ci")
        rows = project.run_sql(
            f"select order_id, doubled from {project.database}.{project.test_schema}.orders_ci"
            " order by order_id",
            fetch="all",
        )
        # only rows changed after the edit get the new column
        assert [(int(order_id), doubled) for order_id, doubled in rows] == [(1, None), (2, 40)]

    def test_switch_to_regular_replaces(self, project):
        created_on = _created_on(project, "orders_ci")
        update_model(project, "orders_ci", REGULAR_MODEL)
        run_dbt(["run"])
        assert _created_on(project, "orders_ci") != created_on
        assert describe_dynamic_table(project, "orders_ci").refresh_mode != "CUSTOM_INCREMENTAL"


class TestCustomIncrementalValidation(_CustomIncrementalBase):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"sources.yml": SOURCES, "orders_ci.sql": _ci_model(unique_key="none")}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        _create_orders(project)
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_merge_without_unique_key_fails(self, project):
        _, logs = run_dbt_and_capture(["run"], expect_pass=False)
        assert "requires `unique_key`" in logs
