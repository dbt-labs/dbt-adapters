import os
import re
import unittest
from unittest import mock
from types import SimpleNamespace

import pytest

from dbt_common.clients.jinja import MaterializationExtension
from jinja2 import Environment, FileSystemLoader

from dbt_common.exceptions import CompilationError

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.include.global_project import PACKAGE_PATH as GLOBAL_PROJECT_PATH

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)

BODY = "select doc_id, body, region from db.sch.docs"
DDL_BODY = "on body attributes region warehouse = WH target_lag = '1 hour' as ( " + BODY + " )"
RELATION = "test_db.test_schema.test_svc"


class _MacroReturn(Exception):
    """dbt's `return()` raises to unwind the macro; plain Jinja's would just render."""

    def __init__(self, value):
        self.value = value


def _raise_return(value):
    raise _MacroReturn(value)


def _raise_compiler_error(message):
    raise ValueError(message)


def _squash(rendered):
    return re.sub(r"\s+", " ", rendered.strip())


def _relation(**kwargs):
    return SnowflakeRelation.create(
        database="test_db", schema="test_schema", identifier="test_svc", **kwargs
    )


class _MacroTestBase(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do", MaterializationExtension],
        )
        self.context = {
            "adapter": mock.Mock(),
            "config": mock.Mock(),
            "return": _raise_return,
            "optional": lambda *args, **kwargs: self.module("utils/optional.sql").optional(
                *args, **kwargs
            ),
        }

    def module(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.context).module


class TestCortexSearchServiceMaterialization(_MacroTestBase):
    def setUp(self):
        super().setUp()
        self.statements = []
        self.grants = {"usage": ["role_a"]}
        self.context.update(
            {
                "this": _relation(),
                "sql": BODY,
                "pre_hooks": [],
                "post_hooks": [],
                "set_query_tag": mock.Mock(return_value="original_tag"),
                "unset_query_tag": mock.Mock(),
                "should_full_refresh": mock.Mock(return_value=False),
                "run_hooks": mock.Mock(return_value=""),
                "statement": self.statement,
                "should_revoke": mock.Mock(return_value=False),
                "apply_grants": mock.Mock(),
                "store_raw_result": mock.Mock(),
            }
        )
        self.context["adapter"].get_cortex_search_service.return_value = None
        self.context["adapter"].get_cortex_search_service_configuration_changes.return_value = {
            "alter": {},
            "replace": [],
        }
        self.config_values = {
            "grants": self.grants,
            "search_column": "body",
            "attributes": ["region"],
            "snowflake_warehouse": "WH",
            "target_lag": "1 hour",
        }
        self.context["config"].get = self.config_values.get
        self.context["config"].model = SimpleNamespace(
            config=SimpleNamespace(
                materialized="cortex_search_service", get=self.config_values.get
            )
        )
        self.context["target"] = SimpleNamespace(warehouse="PROFILE_WH")
        self.context["exceptions"] = SimpleNamespace(
            raise_compiler_error=_raise_compiler_error, warn=mock.Mock()
        )
        with open(os.path.join(GLOBAL_PROJECT_PATH, "macros/utils/literal.sql")) as f:
            literal_macros = self.jinja_env.from_string(f.read()).make_module(self.context)
        with open(os.path.join(GLOBAL_PROJECT_PATH, "macros/utils/escape_single_quotes.sql")) as f:
            escape_macros = self.jinja_env.from_string(f.read()).make_module(self.context)
        dispatched_macros = {
            "string_literal": literal_macros.default__string_literal,
            "escape_single_quotes": self.module(
                "utils/escape_single_quotes.sql"
            ).snowflake__escape_single_quotes,
        }
        self.context["adapter"].dispatch.side_effect = lambda name, package: dispatched_macros[
            name
        ]

        def invoke_macro(macro, *args):
            try:
                return macro(*args)
            except _MacroReturn as returned:
                return returned.value

        self.context["dbt"] = SimpleNamespace(
            string_literal=lambda value: invoke_macro(literal_macros.string_literal, value),
            escape_single_quotes=lambda value: invoke_macro(
                escape_macros.escape_single_quotes, value
            ),
        )
        self.context["snowflake__get_create_cortex_search_service_sql"] = lambda *args: (
            self.module(
                "relations/cortex_search/create.sql"
            ).snowflake__get_create_cortex_search_service_sql(*args)
        )

        self.context["snowflake__get_alter_cortex_search_service_sql"] = lambda *args: self.module(
            "relations/cortex_search/alter.sql"
        ).snowflake__get_alter_cortex_search_service_sql(*args)

        for name in (
            "cortex_search_service_get_build",
            "cortex_search_service_execute_no_op",
            "cortex_search_service_execute_build_sql",
        ):
            self.context[name] = lambda *args, macro_name=name: invoke_macro(
                getattr(self.materialization_module(helpers=True), macro_name),
                *args,
            )

    def statement(self, name, caller=None, **kwargs):
        self.statements.append((name, _squash(caller())))
        return ""

    def materialization_module(self, helpers=False):
        # dbt loads each macro separately; emulate that for plain Jinja's return handling.
        with open(os.path.join(MACROS_DIR, "materializations/cortex_search_service.sql")) as f:
            materialization, end, helper_macros = f.read().partition("{% endmaterialization %}")
        source = helper_macros if helpers else materialization + end
        return self.jinja_env.from_string(source).make_module(self.context)

    def run_materialization(self):
        module = self.materialization_module()
        try:
            module.dbt_macro__materialization_cortex_search_service_snowflake()
        except _MacroReturn as returned:
            return returned.value
        raise AssertionError("materialization did not return()")

    def main_sql(self):
        assert [name for name, _ in self.statements] == ["main"]
        return self.statements[0][1]

    def test_normal_run_creates_if_not_exists(self):
        result = self.run_materialization()
        assert self.main_sql() == (
            f"create cortex search service if not exists {RELATION} {DDL_BODY}"
        )
        assert [r.render() for r in result["relations"]] == [RELATION]
        assert result["relations"][0].is_cortex_search_service
        self.context["adapter"].get_cortex_search_service.assert_called_once_with(
            result["relations"][0]
        )
        self.context["should_revoke"].assert_called_once_with(None, full_refresh_mode=False)

    def test_existing_service_without_drift_is_skipped(self):
        self.context["adapter"].get_cortex_search_service.return_value = _relation(
            type=SnowflakeRelation.CortexSearchService
        )
        self.run_materialization()
        assert self.statements == []
        self.context["store_raw_result"].assert_called_once()
        self.context["exceptions"].warn.assert_not_called()

    def test_full_refresh_creates_or_replaces(self):
        self.context["adapter"].get_cortex_search_service.return_value = _relation(
            type=SnowflakeRelation.CortexSearchService
        )
        self.context["should_full_refresh"].return_value = True
        self.run_materialization()
        assert self.main_sql() == (
            f"create or replace cortex search service {RELATION} {DDL_BODY}"
        )
        self.context["adapter"].get_cortex_search_service_configuration_changes.assert_not_called()

    def test_missing_attributes_raise_before_service_ddl(self):
        self.config_values.pop("attributes")
        with pytest.raises(CompilationError, match="requires a non-empty list for `attributes`"):
            self.run_materialization()
        assert self.statements == []

    def test_multiple_attributes_and_quoted_identifiers(self):
        self.config_values.update(
            search_column='"Body Text"',
            attributes=['"Region Name"', "doc_id"],
            snowflake_warehouse='"Search Warehouse"',
        )
        self.run_materialization()
        assert 'on "Body Text" attributes "Region Name", doc_id' in self.main_sql()
        assert 'warehouse = "Search Warehouse"' in self.main_sql()

    def test_warehouse_defaults_to_profile(self):
        self.config_values.pop("snowflake_warehouse")
        self.run_materialization()
        assert "warehouse = PROFILE_WH" in self.main_sql()

    def test_all_optional_options_in_create_and_replace(self):
        self.config_values.update(
            primary_key=['"Document ID"', "region"],
            embedding_model="snowflake-arctic-embed-m-v1.5",
            refresh_mode="incremental",
            initialize="on_create",
            full_index_build_interval_days=7,
            request_logging=False,
            auto_suspend=1800,
            comment="Support articles",
        )
        options = (
            'on body primary key ("Document ID", region) attributes region '
            "warehouse = WH target_lag = '1 hour' "
            "embedding_model = 'snowflake-arctic-embed-m-v1.5' "
            "refresh_mode = INCREMENTAL initialize = ON_CREATE "
            "full_index_build_interval_days = 7 request_logging = FALSE "
            "auto_suspend = 1800 comment = 'Support articles' "
            f"as ( {BODY} )"
        )
        for full_refresh, prefix in [
            (False, "create cortex search service if not exists"),
            (True, "create or replace cortex search service"),
        ]:
            with self.subTest(full_refresh=full_refresh):
                self.statements.clear()
                self.context["should_full_refresh"].return_value = full_refresh
                self.run_materialization()
                assert self.main_sql() == f"{prefix} {RELATION} {options}"

    def test_optional_options_omitted_when_unset(self):
        names = (
            "primary_key",
            "embedding_model",
            "refresh_mode",
            "initialize",
            "full_index_build_interval_days",
            "request_logging",
            "auto_suspend",
            "comment",
        )
        self.config_values.update(dict.fromkeys(names))
        self.run_materialization()
        assert self.main_sql() == (
            f"create cortex search service if not exists {RELATION} {DDL_BODY}"
        )

    def test_optional_string_literals_preserve_quotes_and_backslashes(self):
        value = "Bob's search \\ notes"
        self.config_values.update(embedding_model=value, comment=value, target_lag=value)
        self.run_materialization()
        for name in ("embedding_model", "comment", "target_lag"):
            assert f"{name} = 'Bob\\'s search \\\\ notes'" in self.main_sql()

    def test_grants_applied_to_service(self):
        existing = _relation(type=SnowflakeRelation.CortexSearchService)
        self.context["adapter"].get_cortex_search_service.return_value = existing
        self.run_materialization()

        self.context["should_revoke"].assert_called_once_with(existing, full_refresh_mode=False)
        self.context["apply_grants"].assert_called_once()
        relation, grant_config = self.context["apply_grants"].call_args.args
        assert relation.is_cortex_search_service
        assert grant_config == self.grants
        assert self.context["apply_grants"].call_args.kwargs == {"should_revoke": False}

    def test_existing_service_grant_reconciliation(self):
        # Exercise dbt's real decision macro, rather than mocking its result.
        with open(os.path.join(GLOBAL_PROJECT_PATH, "macros/adapters/apply_grants.sql")) as f:
            shared_sql = f.read()
        dispatch_adapter = mock.Mock()
        dispatch_adapter.dispatch.return_value = lambda: False  # default copy_grants
        shared = self.jinja_env.from_string(shared_sql).make_module(
            {"return": _raise_return, "adapter": dispatch_adapter}
        )

        def should_revoke(existing_relation, full_refresh_mode=True):
            try:
                shared.should_revoke(existing_relation, full_refresh_mode)
            except _MacroReturn as returned:
                return returned.value

        self.context["should_revoke"] = should_revoke
        self.context["adapter"].get_cortex_search_service.return_value = _relation(
            type=SnowflakeRelation.CortexSearchService
        )
        for full_refresh, expected_revoke in [(False, True), (True, False)]:
            with self.subTest(full_refresh=full_refresh):
                self.context["should_full_refresh"].return_value = full_refresh
                self.run_materialization()
                assert self.context["apply_grants"].call_args.kwargs == {
                    "should_revoke": expected_revoke
                }

    def test_hooks_and_query_tag(self):
        # Hook changes must complete before configured grants are reconciled.
        def run_hooks(hooks):
            self.context["apply_grants"].assert_not_called()
            return ""

        self.context["run_hooks"].side_effect = run_hooks
        self.run_materialization()
        assert self.context["run_hooks"].call_args_list == [
            mock.call(self.context["pre_hooks"]),
            mock.call(self.context["post_hooks"]),
        ]
        self.context["adapter"].commit.assert_not_called()
        self.context["unset_query_tag"].assert_called_once_with("original_tag")


@pytest.mark.parametrize(
    "key,value,expected",
    [
        ("refresh_mode", "full", "refresh_mode = FULL"),
        ("initialize", "on_schedule", "initialize = ON_SCHEDULE"),
        ("request_logging", True, "request_logging = TRUE"),
        ("comment", "", "comment = ''"),
    ],
)
def test_optional_service_option(materialization, key, value, expected):
    materialization.config_values[key] = value
    materialization.run_materialization()
    assert expected in materialization.main_sql()


@pytest.mark.parametrize(
    "value,literal",
    [
        (r"\n", r"'\\n'"),
        ("tail\\", r"'tail\\'"),
        (r"\'", r"'\\\''"),
    ],
)
def test_comment_literals_use_snowflake_escaping(materialization, value, literal):
    test = materialization
    test.config_values["comment"] = value
    test.run_materialization()
    assert f"comment = {literal} as (" in test.main_sql()


def test_missing_warehouse_fails_before_service_ddl(materialization):
    test = materialization
    test.config_values.pop("snowflake_warehouse")
    test.context["target"].warehouse = None
    with pytest.raises(CompilationError, match="snowflake_warehouse"):
        test.run_materialization()
    assert test.statements == []


class TestCortexSearchServiceDiscoveryMacro(_MacroTestBase):
    def test_show_uses_service_namespace_and_escapes_literal(self):
        relation = SnowflakeRelation.create(
            database="Test DB",
            schema="Test Schema",
            identifier="Owner's Search",
            quote_policy={"database": True, "schema": True, "identifier": True},
        )
        self.context["run_query"] = mock.Mock(return_value="services")
        module = self.module("relations/cortex_search/show.sql")
        with self.assertRaises(_MacroReturn) as returned:
            module.snowflake__show_cortex_search_service(relation)
        assert returned.exception.value == "services"
        query = self.context["run_query"].call_args.args[0]
        assert _squash(query) == (
            "show cortex search services like 'Owner''s Search' "
            'in schema "Test DB"."Test Schema"'
        )


class TestCortexSearchServiceDropMacros(_MacroTestBase):
    def test_drop(self):
        rendered = _squash(
            self.module(
                "relations/cortex_search/drop.sql"
            ).snowflake__get_drop_cortex_search_service_sql(RELATION)
        )
        assert rendered == f"drop cortex search service if exists {RELATION}"

    def test_drop_dispatch(self):
        self.context["snowflake__get_drop_cortex_search_service_sql"] = mock.Mock(
            return_value="snowflake__get_drop_cortex_search_service_sql"
        )
        relation = _relation(type=SnowflakeRelation.CortexSearchService)
        rendered = self.module("relations/drop.sql").snowflake__get_drop_sql(relation)
        assert rendered.strip() == "snowflake__get_drop_cortex_search_service_sql"


class TestCortexSearchServiceGrantMacros(_MacroTestBase):
    def setUp(self):
        super().setUp()
        self.context["adapter"].quote = lambda value: f'"{value}"'
        for name in [
            "default__get_show_grant_sql",
            "default__get_grant_sql",
            "default__get_revoke_sql",
        ]:
            self.context[name] = mock.Mock(return_value=name)
        self.grants = self.module("apply_grants.sql")
        self.service = _relation(type=SnowflakeRelation.CortexSearchService)

    def test_show_grants(self):
        assert _squash(self.grants.snowflake__get_show_grant_sql(self.service)) == (
            f"show grants on cortex search service {RELATION}"
        )

    def test_grant(self):
        assert _squash(
            self.grants.snowflake__get_grant_sql(self.service, "usage", ["r1", "r2"])
        ) == (f"grant usage on cortex search service {RELATION} to r1, r2")

    def test_revoke_quotes_grantees(self):
        assert _squash(self.grants.snowflake__get_revoke_sql(self.service, "usage", ["r1"])) == (
            f'revoke usage on cortex search service {RELATION} from "r1"'
        )

    def test_non_service_relations_use_defaults(self):
        table = _relation(type=SnowflakeRelation.Table)
        assert (
            self.grants.snowflake__get_show_grant_sql(table).strip()
            == "default__get_show_grant_sql"
        )
        assert (
            self.grants.snowflake__get_grant_sql(table, "select", ["r"])
            == "default__get_grant_sql"
        )
        assert (
            self.grants.snowflake__get_revoke_sql(table, "select", ["r"])
            == "default__get_revoke_sql"
        )


@pytest.fixture
def materialization():
    test = TestCortexSearchServiceMaterialization()
    test.setUp()
    return test


@pytest.mark.parametrize("replacement", [False, True], ids=["alter", "replace"])
@pytest.mark.parametrize("policy", ["apply", "continue", "fail"])
def test_configuration_drift_obeys_policy(materialization, replacement, policy):
    test = materialization
    test.config_values.update(on_configuration_change=policy, target_lag="2 hours")
    existing = _relation(type=SnowflakeRelation.CortexSearchService)
    test.context["adapter"].get_cortex_search_service.return_value = existing
    if replacement:
        test.config_values["attributes"] = ["region", "doc_id"]
        changes = {"alter": {"target_lag": "2 hours"}, "replace": ["attributes"]}
    else:
        changes = {
            "alter": {
                "target_lag": "2 hours",
                "warehouse": '"Search Warehouse"',
                "request_logging": False,
                "comment": r"Bob's \ notes",
            },
            "replace": [],
        }
    test.context["adapter"].get_cortex_search_service_configuration_changes.return_value = changes
    if policy == "fail":
        with pytest.raises(ValueError, match="configuration changed.*target_lag.*fail"):
            test.run_materialization()
        assert test.statements == []
        test.context["apply_grants"].assert_not_called()
        return

    test.run_materialization()
    test.context["should_revoke"].assert_called_once_with(
        existing, full_refresh_mode=(policy == "apply" and replacement)
    )
    if policy == "continue":
        assert test.statements == []
        test.context["store_raw_result"].assert_called_once()
        test.context["exceptions"].warn.assert_called_once()
        assert "target_lag" in test.context["exceptions"].warn.call_args.args[0]
    elif replacement:
        assert test.main_sql() == (
            f"create or replace cortex search service {RELATION} "
            "on body attributes region, doc_id warehouse = WH "
            f"target_lag = '2 hours' as ( {BODY} )"
        )
    else:
        assert test.main_sql() == (
            f"alter cortex search service {RELATION} set "
            "target_lag = '2 hours' warehouse = \"Search Warehouse\" "
            "request_logging = FALSE comment = 'Bob\\'s \\\\ notes'"
        )
