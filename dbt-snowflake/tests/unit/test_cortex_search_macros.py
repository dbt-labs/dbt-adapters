import os
import re
import unittest
from unittest import mock
from types import SimpleNamespace

import pytest

from dbt_common.clients.jinja import MaterializationExtension
from jinja2 import Environment, FileSystemLoader

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
        }

    def module(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.context).module


class TestCortexSearchServiceMaterialization(_MacroTestBase):
    def setUp(self):
        super().setUp()
        self.statements = []
        self.logs = []
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
                "log": lambda msg, info=False: self.logs.append((msg, info)) or "",
                "should_revoke": mock.Mock(return_value=False),
                "apply_grants": mock.Mock(),
            }
        )
        self.context["adapter"].get_cortex_search_service.return_value = None
        self.config_values = {
            "grants": self.grants,
            "search_column": "body",
            "attributes": ["region"],
            "snowflake_warehouse": "WH",
            "target_lag": "1 hour",
        }
        self.context["config"].get = self.config_values.get
        self.context["target"] = SimpleNamespace(warehouse="PROFILE_WH")
        self.context["exceptions"] = SimpleNamespace(raise_compiler_error=_raise_compiler_error)
        self.context["snowflake__get_create_cortex_search_service_sql"] = lambda *args: (
            self.module(
                "relations/cortex_search/create.sql"
            ).snowflake__get_create_cortex_search_service_sql(*args)
        )

    def statement(self, name, caller=None, **kwargs):
        self.statements.append((name, _squash(caller())))
        return ""

    def run_materialization(self):
        module = self.module("materializations/cortex_search_service.sql")
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

    def test_normal_run_logs_full_refresh_hint(self):
        self.run_materialization()
        assert any(
            "kept unchanged" in msg and "--full-refresh" in msg and info for msg, info in self.logs
        )

    def test_full_refresh_creates_or_replaces(self):
        self.context["should_full_refresh"].return_value = True
        self.run_materialization()
        assert self.main_sql() == (
            f"create or replace cortex search service {RELATION} {DDL_BODY}"
        )
        assert self.logs == []

    def test_never_uses_create_or_alter(self):
        for full_refresh in (False, True):
            self.statements.clear()
            self.context["should_full_refresh"].return_value = full_refresh
            self.run_materialization()
            assert "create or alter" not in self.main_sql()

    def test_missing_attributes_raise_before_service_ddl(self):
        self.config_values.pop("attributes")
        with pytest.raises(ValueError, match="requires a non-empty list for `attributes`"):
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

    def test_target_lag_literal_is_escaped(self):
        self.config_values["target_lag"] = "one's hour"
        self.run_materialization()
        assert "target_lag = 'one''s hour'" in self.main_sql()

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
            assert f"{name} = 'Bob''s search \\\\ notes'" in self.main_sql()

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

    def test_grants_render_cortex_search_grant_sql(self):
        """The relation handed to apply_grants renders the cortex search grant DCL."""
        self.run_materialization()
        relation, grant_config = self.context["apply_grants"].call_args.args
        grant_macros = self.module("apply_grants.sql")
        rendered = [
            _squash(grant_macros.snowflake__get_grant_sql(relation, privilege, [grantee]))
            for privilege, grantees in grant_config.items()
            for grantee in grantees
        ]
        assert rendered == [f"grant usage on cortex search service {RELATION} to role_a"]

    def test_missing_cached_relation_does_not_revoke(self):
        self.run_materialization()
        self.context["should_revoke"].assert_called_once_with(None, full_refresh_mode=False)

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

    def test_service_lookup_uses_service_type(self):
        self.run_materialization()
        self.context["adapter"].get_cortex_search_service.assert_called_once()
        relation = self.context["adapter"].get_cortex_search_service.call_args.args[0]
        assert relation.is_cortex_search_service

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
    "key,value",
    [
        ("search_column", None),
        ("search_column", " "),
        ("search_column", ["body"]),
        ("target_lag", None),
        ("target_lag", ""),
        ("target_lag", 60),
        ("snowflake_warehouse", " "),
        ("snowflake_warehouse", 123),
        ("attributes", "region"),
        ("attributes", None),
        ("attributes", []),
        ("attributes", {"region": True}),
        ("attributes", [""]),
        ("attributes", [123]),
        ("primary_key", "doc_id"),
        ("primary_key", {"doc_id": True}),
        ("primary_key", [""]),
        ("primary_key", [123]),
        ("embedding_model", ""),
        ("embedding_model", 123),
        ("refresh_mode", "AUTO"),
        ("refresh_mode", ["FULL"]),
        ("initialize", "IMMEDIATELY"),
        ("initialize", True),
        ("full_index_build_interval_days", 0),
        ("full_index_build_interval_days", -1),
        ("full_index_build_interval_days", 1.5),
        ("full_index_build_interval_days", True),
        ("full_index_build_interval_days", "7"),
        ("auto_suspend", 1799),
        ("auto_suspend", -1),
        ("auto_suspend", 1800.5),
        ("auto_suspend", True),
        ("auto_suspend", "1800"),
        ("request_logging", "false"),
        ("request_logging", 0),
        ("request_logging", 1),
        ("comment", 123),
    ],
)
def test_invalid_service_config_fails_before_service_ddl(key, value):
    test = TestCortexSearchServiceMaterialization()
    test.setUp()
    test.config_values[key] = value
    with pytest.raises(ValueError, match=key):
        test.run_materialization()
    assert test.statements == []


@pytest.mark.parametrize(
    "key,value,expected",
    [
        ("primary_key", ["doc_id", '"Region Name"'], 'primary key (doc_id, "Region Name")'),
        (
            "embedding_model",
            "snowflake-arctic-embed-l-v2.0",
            "embedding_model = 'snowflake-arctic-embed-l-v2.0'",
        ),
        ("refresh_mode", "full", "refresh_mode = FULL"),
        ("refresh_mode", " INCREMENTAL ", "refresh_mode = INCREMENTAL"),
        ("initialize", "on_schedule", "initialize = ON_SCHEDULE"),
        ("initialize", " ON_CREATE ", "initialize = ON_CREATE"),
        ("full_index_build_interval_days", 1, "full_index_build_interval_days = 1"),
        ("request_logging", True, "request_logging = TRUE"),
        ("request_logging", False, "request_logging = FALSE"),
        ("auto_suspend", 1800, "auto_suspend = 1800"),
        ("auto_suspend", 3600, "auto_suspend = 3600"),
        ("comment", "", "comment = ''"),
    ],
)
def test_optional_service_option(key, value, expected):
    test = TestCortexSearchServiceMaterialization()
    test.setUp()
    test.config_values[key] = value
    if key == "full_index_build_interval_days":
        test.config_values["primary_key"] = ["doc_id"]
    test.run_materialization()
    assert expected in test.main_sql()


@pytest.mark.parametrize("primary_key", [None, []])
def test_index_rebuild_interval_requires_primary_key(primary_key):
    test = TestCortexSearchServiceMaterialization()
    test.setUp()
    test.config_values.update(primary_key=primary_key, full_index_build_interval_days=7)
    with pytest.raises(ValueError, match="requires a non-empty `primary_key`"):
        test.run_materialization()
    assert test.statements == []


def test_missing_warehouse_fails_before_service_ddl():
    test = TestCortexSearchServiceMaterialization()
    test.setUp()
    test.config_values.pop("snowflake_warehouse")
    test.context["target"].warehouse = None
    with pytest.raises(ValueError, match="snowflake_warehouse"):
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
