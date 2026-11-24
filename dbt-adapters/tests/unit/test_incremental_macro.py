from unittest import mock
import pytest
from jinja2 import Environment, FileSystemLoader
from dbt_common.clients.jinja import MaterializationExtension


class TestIncrementalExpandTargetColumnTypes:
    @pytest.fixture
    def jinja_env(self):
        return Environment(
            loader=FileSystemLoader("src/dbt/include/global_project/macros"),
            extensions=[MaterializationExtension, "jinja2.ext.do"],
        )

    def _run_incremental_macro(self, jinja_env, on_schema_change="sync_all_columns", contract_enforced=False):
        adapter = mock.Mock()
        adapter.expand_target_column_types = mock.Mock(return_value="")
        adapter.commit = mock.Mock(return_value="")
        adapter.drop_relation = mock.Mock(return_value="")
        adapter.rename_relation = mock.Mock(return_value="")
        adapter.get_incremental_strategy_macro.return_value = lambda arg_dict: "-- strategy sql"

        def _make_mock_relation(name):
            rel = mock.Mock()
            rel.__str__ = lambda s: name
            rel.render.return_value = name
            rel.type = "table"
            rel.is_view = False
            return rel

        existing_relation = _make_mock_relation("existing_relation")
        target_relation = _make_mock_relation("target_relation")
        temp_relation = _make_mock_relation("temp_relation")
        intermediate_relation = _make_mock_relation("intermediate_relation")
        backup_relation = _make_mock_relation("backup_relation")

        this_relation = mock.Mock()
        this_relation.incorporate.return_value = target_relation

        config_values = {
            "unique_key": "id",
            "on_schema_change": on_schema_change,
            "contract": {"enforced": contract_enforced},
            "grants": {},
            "incremental_strategy": "default",
        }

        config = mock.Mock()
        config.get = lambda key, default=None: config_values.get(key, default)

        def _mock_statement(name, **kwargs):
            caller = kwargs.get("caller")
            if caller:
                caller()
            return ""

        context = {
            "this": this_relation,
            "config": config,
            "model": mock.Mock(),
            "sql": "select 1 as id",
            "adapter": adapter,
            "load_cached_relation": lambda r: existing_relation,
            "make_temp_relation": lambda r: temp_relation,
            "make_intermediate_relation": lambda r: intermediate_relation,
            "make_backup_relation": lambda r, t: backup_relation,
            "should_full_refresh": lambda: False,
            "incremental_validate_on_schema_change": lambda val, default="ignore": val or default,
            "drop_relation_if_exists": lambda r: "",
            "run_hooks": lambda hooks, **kwargs: "",
            "pre_hooks": [],
            "post_hooks": [],
            "get_create_table_as_sql": lambda temp, rel, sql: f"create table as {rel}",
            "run_query": lambda q: "",
            "process_schema_changes": lambda osc, s, t: ["id"],
            "statement": _mock_statement,
            "create_indexes": lambda r: "",
            "should_revoke": lambda existing, full_refresh: False,
            "apply_grants": lambda rel, grant_config, **kwargs: "",
            "persist_docs": lambda rel, model: "",
            "return": lambda val: val,
        }

        template = jinja_env.get_template(
            "materializations/models/incremental/incremental.sql", globals=context
        )
        template.module.dbt_macro__materialization_incremental_default()
        return adapter.expand_target_column_types

    def test_skips_expand_target_column_types_when_sync_all_columns(self, jinja_env):
        """When on_schema_change is sync_all_columns, expand_target_column_types is redundant (resolves #2216)."""
        expand_mock = self._run_incremental_macro(
            jinja_env, on_schema_change="sync_all_columns", contract_enforced=False
        )
        assert not expand_mock.called

    def test_calls_expand_target_column_types_when_append_new_columns(self, jinja_env):
        expand_mock = self._run_incremental_macro(
            jinja_env, on_schema_change="append_new_columns", contract_enforced=False
        )
        assert expand_mock.called

    def test_calls_expand_target_column_types_when_ignore(self, jinja_env):
        expand_mock = self._run_incremental_macro(
            jinja_env, on_schema_change="ignore", contract_enforced=False
        )
        assert expand_mock.called

    def test_skips_expand_target_column_types_when_contract_enforced(self, jinja_env):
        expand_mock = self._run_incremental_macro(
            jinja_env, on_schema_change="append_new_columns", contract_enforced=True
        )
        assert not expand_mock.called
