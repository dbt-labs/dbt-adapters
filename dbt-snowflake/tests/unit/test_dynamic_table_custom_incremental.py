"""
Unit tests for custom incremental dynamic tables (CI DT): `refresh_mode='CUSTOM_INCREMENTAL'`
with `incremental_strategy='merge'`, and the `changes()` macro.

The macros render with the real templates (this adapter's and the global project's
`get_merge_update_columns`) and a mocked dbt context. dbt's `return()` unwinds the macro it is
called in, so `Macro._invoke` is patched to turn `_DbtReturn` into the macro's value, as dbt's
MacroGenerator does.
"""

import os
import re
from types import SimpleNamespace
from unittest import mock

import jinja2.runtime
import pytest
from jinja2 import Environment, FileSystemLoader

import dbt.include.global_project as global_project
from dbt.adapters.snowflake.column import SnowflakeColumn

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)
GLOBAL_MACROS_DIR = os.path.join(global_project.PACKAGE_PATH, "macros")

TEMPLATES = [
    "utils/optional.sql",
    "utils/changes.sql",
    "utils/list_columns.sql",
    "relations/dynamic_table/custom_incremental.sql",
    "relations/dynamic_table/create.sql",
    "relations/dynamic_table/replace.sql",
    "relations/dynamic_table/create_or_alter.sql",
    "materializations/incremental/merge.sql",
    "materializations/models/incremental/column_helpers.sql",
]

MODEL_SQL = (
    "select o.order_id, o.amount\n" "from db.raw.orders changes(information => DEFAULT) as o"
)

COLUMNS = [
    SnowflakeColumn(column="ORDER_ID", dtype="NUMBER", numeric_precision=38, numeric_scale=0),
    SnowflakeColumn(column="AMOUNT", dtype="NUMBER", numeric_precision=10, numeric_scale=2),
]


class _DbtReturn(Exception):
    def __init__(self, value):
        self.value = value


class _CompilerError(Exception):
    pass


_original_invoke = jinja2.runtime.Macro._invoke


def _invoke_with_return(self, arguments, autoescape):
    try:
        return _original_invoke(self, arguments, autoescape)
    except _DbtReturn as dbt_return:
        return dbt_return.value


def _raise_return(value):
    raise _DbtReturn(value)


def _raise_compiler_error(msg):
    raise _CompilerError(msg)


def _dynamic_table(**overrides):
    """A stand-in for SnowflakeDynamicTableConfig with the attributes the DDL macros read."""
    values = {
        "transient": None,
        "target_lag": "1 hour",
        "warehouse_parameter": "wh",
        "snowflake_initialization_warehouse": None,
        "refresh_mode": "CUSTOM_INCREMENTAL",
        "initialize": None,
        "scheduler": None,
        "row_access_policy": None,
        "table_tag": None,
        "cluster_by": None,
        "copy_grants": None,
        "immutable_where": None,
    }
    values.update(overrides)
    return mock.Mock(**values)


def _squash(sql):
    return re.sub(r"\s+", " ", sql).strip()


class _Renderer:
    """Loads every template into one dbt-like namespace and records the statements run."""

    def __init__(self, model_config, columns=COLUMNS, catalog_type="INFO_SCHEMA", empty=False):
        self.statements = []
        self.dropped = []
        self.macros = {}

        config = mock.Mock(model=mock.Mock())
        config.get.side_effect = lambda key, default=None: model_config.get(key, default)

        adapter = mock.Mock()
        adapter.behavior.snowflake_default_transient_dynamic_tables.no_warn = False
        adapter.build_catalog_relation.return_value = mock.Mock(catalog_type=catalog_type)
        adapter.get_columns_in_relation.return_value = columns
        adapter.drop_relation.side_effect = self.dropped.append
        adapter.dispatch.side_effect = lambda name, namespace=None: (
            self.macros.get(f"snowflake__{name}") or self.macros[f"default__{name}"]
        )

        temp_relation = mock.Mock()
        temp_relation.incorporate.return_value = "db.sch.dt__dbt_tmp"

        self.context = {
            "adapter": adapter,
            "config": config,
            "this": "db.sch.dt",
            "flags": SimpleNamespace(EMPTY=empty),
            "exceptions": SimpleNamespace(raise_compiler_error=_raise_compiler_error),
            "return": _raise_return,
            "statement": self._statement,
            "make_temp_relation": lambda relation: temp_relation,
            "create_table_as": lambda temporary, relation, sql: (
                f"create temporary table {relation} as ({sql})"
            ),
            # Lives in materializations/incremental.sql, which plain Jinja can't load; outside a
            # Glue catalog it returns the names unchanged.
            "snowflake__quote_glue_columns": lambda names, dest_columns, catalog_relation: names,
        }

        env = Environment(
            loader=FileSystemLoader([MACROS_DIR, GLOBAL_MACROS_DIR]),
            extensions=["jinja2.ext.do"],
        )
        # A macro resolves names from other templates when its template loads, so each macro
        # name is bound to a proxy first and the proxies are filled in afterwards.
        for template in TEMPLATES:
            source = env.loader.get_source(env, template)[0]
            for name in re.findall(r"{%-?\s*macro\s+(\w+)\s*\(", source):
                self.context[name] = self._proxy(name)
        for template in TEMPLATES:
            module = env.get_template(template, globals=self.context).module
            for name in dir(module):
                if isinstance(getattr(module, name), jinja2.runtime.Macro):
                    self.macros[name] = getattr(module, name)

    def _proxy(self, name):
        return lambda *args, **kwargs: self.macros[name](*args, **kwargs)

    def _statement(self, name=None, fetch_result=False, auto_begin=True, caller=None):
        self.statements.append(caller())
        return ""

    def __call__(self, macro_name, *args):
        with mock.patch.object(jinja2.runtime.Macro, "_invoke", _invoke_with_return):
            return self.macros[macro_name](*args)


def _ci_config(**overrides):
    values = {
        "refresh_mode": "custom_incremental",
        "incremental_strategy": "merge",
        "unique_key": "order_id",
    }
    values.update(overrides)
    return values


def _refresh_using_body(ddl):
    return ddl[ddl.index("refresh using (") :].rstrip()


class TestChanges:
    @staticmethod
    def _changes(relation, mode, empty=False):
        renderer = _Renderer({}, empty=empty)
        return renderer("changes", relation, mode).strip()

    def test_default_mode_renders_unbounded_changes(self):
        assert (
            self._changes("db.raw.orders", "default")
            == "db.raw.orders changes(information => DEFAULT)"
        )

    def test_append_only_mode_is_case_insensitive(self):
        assert (
            self._changes("db.raw.orders", "APPEND_only")
            == "db.raw.orders changes(information => APPEND_ONLY)"
        )

    def test_mode_is_required(self):
        with pytest.raises(_CompilerError, match="`mode` is required"):
            self._changes("db.raw.orders", None)

    def test_unknown_mode_raises(self):
        with pytest.raises(_CompilerError, match="unsupported mode 'latest'"):
            self._changes("db.raw.orders", "latest")

    def test_a_relation_renders_its_plain_name(self):
        # `{{ relation }}` renders a filtered subquery under --empty / --sample, which CHANGES
        # can't follow.
        relation = mock.Mock()
        relation.render.return_value = "db.raw.orders"
        relation.__str__ = lambda self: "(select * from db.raw.orders where false limit 0)"
        assert (
            self._changes(relation, "default") == "db.raw.orders changes(information => DEFAULT)"
        )

    def test_empty_run_limits_after_changes(self):
        assert (
            self._changes("db.raw.orders", "default", empty=True)
            == "(select * from db.raw.orders changes(information => DEFAULT) limit 0)"
        )


class TestCustomIncrementalDdl:
    def test_create_discovers_columns_and_stores_a_merge_body(self):
        renderer = _Renderer(_ci_config())
        ddl = renderer(
            "snowflake__create_dynamic_table_info_schema_sql",
            _dynamic_table(),
            "db.sch.dt",
            MODEL_SQL,
        )

        assert renderer.statements == [
            "create temporary table db.sch.dt__dbt_tmp as (select * from (\n"
            "select o.order_id, o.amount\n"
            "from db.raw.orders changes(information => DEFAULT) at(offset => 0) as o\n"
            ") limit 0)"
        ]
        assert renderer.dropped == ["db.sch.dt__dbt_tmp"]
        assert _squash(ddl).startswith(
            'create dynamic table db.sch.dt ("ORDER_ID" NUMBER(38,0), "AMOUNT" NUMBER(10,2))'
        )
        assert "refresh_mode = CUSTOM_INCREMENTAL" in ddl
        assert " as (" not in ddl
        assert _refresh_using_body(ddl) == (
            "refresh using (\n"
            "merge into SELF as DBT_INTERNAL_DEST\n"
            "using (\n" + MODEL_SQL + "\n) as DBT_INTERNAL_SOURCE\n"
            "on DBT_INTERNAL_DEST.order_id = DBT_INTERNAL_SOURCE.order_id\n"
            'when matched then update set "ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID", '
            '"AMOUNT" = DBT_INTERNAL_SOURCE."AMOUNT"\n'
            'when not matched then insert ("ORDER_ID", "AMOUNT") '
            'values (DBT_INTERNAL_SOURCE."ORDER_ID", DBT_INTERNAL_SOURCE."AMOUNT")\n'
            "    )"
        )

    def test_body_is_byte_identical_across_create_replace_and_create_or_alter(self):
        # Snowflake compares the stored definition literally, so CREATE OR ALTER must send the
        # exact `refresh using (...)` text CREATE and CREATE OR REPLACE stored.
        renderer = _Renderer(_ci_config())
        args = (_dynamic_table(), "db.sch.dt", MODEL_SQL)
        create = renderer("snowflake__create_dynamic_table_info_schema_sql", *args)
        replace = renderer("snowflake__replace_dynamic_table_info_schema_sql", *args)
        create_or_alter = renderer(
            "snowflake__create_or_alter_dynamic_table_info_schema_sql", *args
        )

        assert _squash(replace).startswith("create or replace dynamic table db.sch.dt (")
        assert _squash(create_or_alter).startswith("create or alter dynamic table db.sch.dt (")
        assert _refresh_using_body(create) == _refresh_using_body(replace)
        assert _refresh_using_body(create) == _refresh_using_body(create_or_alter)

    def test_regular_dynamic_table_is_unchanged(self):
        renderer = _Renderer({"incremental_strategy": "merge", "unique_key": "order_id"})
        ddl = renderer(
            "snowflake__create_dynamic_table_info_schema_sql",
            _dynamic_table(refresh_mode=None),
            "db.sch.dt",
            MODEL_SQL,
        )
        assert renderer.statements == []
        assert "refresh using" not in ddl
        assert "\n    as (\n        " + MODEL_SQL + "\n    )" in ddl

    def test_composite_key_and_delete_condition(self):
        renderer = _Renderer(
            _ci_config(
                unique_key=["order_id", "region"],
                delete_condition="DBT_INTERNAL_SOURCE.is_deleted",
            )
        )
        body = renderer("snowflake__custom_incremental_refresh_using_sql", MODEL_SQL, COLUMNS)
        assert (
            "on DBT_INTERNAL_DEST.order_id = DBT_INTERNAL_SOURCE.order_id "
            "and DBT_INTERNAL_DEST.region = DBT_INTERNAL_SOURCE.region\n"
            "when matched and coalesce(DBT_INTERNAL_SOURCE.is_deleted, false) then delete\n"
            "when matched then update set"
        ) in body
        assert (
            "when not matched and not coalesce(DBT_INTERNAL_SOURCE.is_deleted, false) then insert"
            in body
        )

    def test_merge_update_columns_narrow_the_update(self):
        renderer = _Renderer(_ci_config(merge_update_columns=["AMOUNT"]))
        body = renderer("snowflake__custom_incremental_refresh_using_sql", MODEL_SQL, COLUMNS)
        assert "when matched then update set AMOUNT = DBT_INTERNAL_SOURCE.AMOUNT\n" in body

    def test_merge_exclude_columns_are_not_updated(self):
        renderer = _Renderer(_ci_config(merge_exclude_columns=["amount"]))
        body = renderer("snowflake__custom_incremental_refresh_using_sql", MODEL_SQL, COLUMNS)
        assert 'when matched then update set "ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID"\n' in body

    def test_merge_update_and_exclude_columns_together_raise(self):
        renderer = _Renderer(
            _ci_config(merge_update_columns=["AMOUNT"], merge_exclude_columns=["ORDER_ID"])
        )
        with pytest.raises(_CompilerError, match="merge_update_columns and merge_exclude_columns"):
            renderer("snowflake__custom_incremental_refresh_using_sql", MODEL_SQL, COLUMNS)


class TestCustomIncrementalDiscovery:
    def test_discovery_bounds_both_changes_modes(self):
        sql = (
            "select * from a changes(information => DEFAULT)\n"
            "union all select * from b changes(information => APPEND_ONLY)"
        )
        renderer = _Renderer(_ci_config())
        renderer("snowflake__custom_incremental_discover_columns", "db.sch.dt", sql)
        assert "a changes(information => DEFAULT) at(offset => 0)" in renderer.statements[0]
        assert "b changes(information => APPEND_ONLY) at(offset => 0)" in renderer.statements[0]

    def test_hand_written_changes_is_discovered_as_written(self):
        sql = "select * from a changes(information => default)"
        renderer = _Renderer(_ci_config())
        renderer("snowflake__custom_incremental_discover_columns", "db.sch.dt", sql)
        assert "at(offset => 0)" not in renderer.statements[0]

    @pytest.mark.parametrize("name", ["METADATA$ACTION", "metadata$isupdate", "METADATA$ROW_ID"])
    def test_metadata_column_in_the_output_is_rejected(self, name):
        renderer = _Renderer(_ci_config(), columns=[SnowflakeColumn(column=name, dtype="TEXT")])
        with pytest.raises(_CompilerError, match="Alias it in the model's SELECT"):
            renderer("snowflake__custom_incremental_discover_columns", "db.sch.dt", MODEL_SQL)


class TestCustomIncrementalValidation:
    @staticmethod
    def _validate(model_config, catalog_type="INFO_SCHEMA"):
        renderer = _Renderer(model_config, catalog_type=catalog_type)
        renderer("snowflake__validate_custom_incremental_config")

    def test_valid_config_passes(self):
        self._validate(_ci_config())

    def test_strategy_is_case_insensitive(self):
        self._validate(_ci_config(incremental_strategy="MERGE"))

    def test_requires_an_incremental_strategy(self):
        with pytest.raises(_CompilerError, match="no `incremental_strategy`"):
            self._validate(_ci_config(incremental_strategy=None))

    def test_rejects_strategies_other_than_merge(self):
        with pytest.raises(_CompilerError, match="unsupported incremental_strategy 'append'"):
            self._validate(_ci_config(incremental_strategy="append"))

    def test_cannot_be_an_iceberg_table(self):
        with pytest.raises(_CompilerError, match="dynamic Iceberg tables"):
            self._validate(_ci_config(), catalog_type="BUILT_IN")

    def test_rejects_immutable_where(self):
        with pytest.raises(_CompilerError, match="immutable_where"):
            self._validate(_ci_config(immutable_where="ts < '2026-01-01'"))

    def test_merge_requires_a_unique_key(self):
        with pytest.raises(_CompilerError, match="requires `unique_key`"):
            self._validate(_ci_config(unique_key=None))

    def test_delete_condition_cannot_reference_the_target_row(self):
        with pytest.raises(_CompilerError, match="not DBT_INTERNAL_DEST"):
            self._validate(_ci_config(delete_condition="dbt_internal_dest.is_deleted"))

    @pytest.mark.parametrize(
        "refresh_mode,expected",
        [
            ("CUSTOM_INCREMENTAL", True),
            ("custom_incremental", True),
            ("AUTO", False),
            (None, False),
        ],
    )
    def test_is_custom_incremental(self, refresh_mode, expected):
        renderer = _Renderer({"refresh_mode": refresh_mode})
        assert renderer("snowflake__dynamic_table_is_custom_incremental") is expected
