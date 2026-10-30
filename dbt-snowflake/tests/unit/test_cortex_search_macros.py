from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import agate
import pytest
from dbt_common.clients.jinja import MaterializationExtension
from dbt_common.exceptions import CompilationError
from jinja2 import Environment

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.include.global_project import PACKAGE_PATH as GLOBAL_PROJECT_PATH
from dbt.include.snowflake import PACKAGE_PATH

MACROS = Path(PACKAGE_PATH) / "macros"
SERVICE = SnowflakeRelation.create(
    database="db", schema="sch", identifier="search", type=SnowflakeRelation.CortexSearchService
)
QUERY = "select body, region from docs"
MATERIALIZATION = "materializations/cortex_search_service.sql"


class MacroReturn(Exception):
    def __init__(self, value):
        self.value = value


def return_value(value):
    raise MacroReturn(value)


def compiler_error(message):
    raise CompilationError(message)


@pytest.fixture
def config_values():
    return {"search_column": "body", "attributes": ["region"], "target_lag": "1 hour"}


@pytest.fixture
def context(config_values):
    adapter = mock.Mock()
    adapter.check_schema_exists.return_value = True
    adapter.quote.side_effect = lambda value: f'"{value}"'
    return {
        "snowflake__describe_cortex_search_service": mock.Mock(
            return_value={
                "search_column": "BODY",
                "attribute_columns": "REGION",
                "warehouse": "WH",
                "target_lag": "1 hour",
            }
        ),
        "adapter": adapter,
        "config": SimpleNamespace(
            get=config_values.get,
            model=SimpleNamespace(
                config=SimpleNamespace(materialized="cortex_search_service", get=config_values.get)
            ),
        ),
        "target": SimpleNamespace(warehouse="WH"),
        "sql": QUERY,
        "return": return_value,
        "exceptions": SimpleNamespace(
            raise_compiler_error=mock.Mock(side_effect=compiler_error), warn=mock.Mock()
        ),
    }


@pytest.fixture
def render(context):
    env = Environment(extensions=["jinja2.ext.do", MaterializationExtension])

    def call(path, name, *args):
        source = (MACROS / path).read_text()
        # dbt loads macros separately; keep nested return() calls inside their own wrapper.
        if "{% endmaterialization %}" in source:
            main, end, helpers = source.partition("{% endmaterialization %}")
            source = main + end if name.startswith("dbt_macro__") else helpers
        try:
            result = getattr(env.from_string(source).make_module(context), name)(*args)
        except MacroReturn as returned:
            result = returned.value
        return result

    context["optional"] = lambda *args: call("utils/optional.sql", "optional", *args)
    context["dbt"] = SimpleNamespace(
        string_literal=lambda value: call(
            Path(GLOBAL_PROJECT_PATH) / "macros/utils/literal.sql",
            "default__string_literal",
            value,
        ),
        escape_single_quotes=lambda value: call(
            "utils/escape_single_quotes.sql", "snowflake__escape_single_quotes", value
        ),
    )
    for action in ("create", "alter"):
        name = f"snowflake__get_{action}_cortex_search_service_sql"
        context[name] = lambda *args, action=action, name=name: call(
            f"relations/cortex_search/{action}.sql", name, *args
        )

    def render_sql(path, name, *args):
        result = call(path, name, *args)
        return " ".join(result.split()) if isinstance(result, str) else result

    return render_sql


@pytest.mark.parametrize("full_refresh", [False, True])
@pytest.mark.parametrize("options", [False, True])
def test_create_sql(config_values, render, full_refresh, options):
    expected = "on body attributes region warehouse = WH target_lag = '1 hour'"
    if options:
        config_values.update(
            search_column='"Body Text"',
            attributes=['"Region Name"', "doc_id"],
            snowflake_warehouse='"Search Warehouse"',
            primary_key=["doc_id"],
            embedding_model="snowflake-arctic-embed-m-v1.5",
            refresh_mode=" incremental ",
            initialize=" on_schedule ",
            full_index_build_interval_days=1,
            request_logging=False,
            auto_suspend=1800,
            comment="",
        )
        expected = (
            'on "Body Text" primary key (doc_id) attributes "Region Name", doc_id '
            "warehouse = \"Search Warehouse\" target_lag = '1 hour' "
            "embedding_model = 'snowflake-arctic-embed-m-v1.5' refresh_mode = INCREMENTAL "
            "initialize = ON_SCHEDULE full_index_build_interval_days = 1 "
            "request_logging = FALSE auto_suspend = 1800 comment = ''"
        )
    prefix = (
        "create or replace cortex search service"
        if full_refresh
        else "create cortex search service if not exists"
    )
    assert (
        render(
            "relations/cortex_search/create.sql",
            "snowflake__get_create_cortex_search_service_sql",
            SERVICE,
            QUERY,
            full_refresh,
        )
        == f"{prefix} {SERVICE.render()} {expected} as ( {QUERY} )"
    )


def test_alter_sql(render):
    changes = {
        "target_lag": "2 hours",
        "warehouse": '"Search Warehouse"',
        "request_logging": True,
        "auto_suspend": 3600,
        "full_index_build_interval_days": 3,
        "comment": r"Bob's \ notes",
    }
    assert render(
        "relations/cortex_search/alter.sql",
        "snowflake__get_alter_cortex_search_service_sql",
        SERVICE,
        changes,
    ) == (
        f"alter cortex search service {SERVICE.render()} set target_lag = '2 hours' "
        'warehouse = "Search Warehouse" request_logging = TRUE auto_suspend = 3600 '
        r"full_index_build_interval_days = 3 comment = 'Bob\'s \\ notes'"
    )


def test_create_escapes_string_options(config_values, render):
    config_values.update(
        dict.fromkeys(["target_lag", "embedding_model", "comment"], "Bob's \\ notes\\")
    )
    sql = render(
        "relations/cortex_search/create.sql",
        "snowflake__get_create_cortex_search_service_sql",
        SERVICE,
        QUERY,
    )
    for name in ("target_lag", "embedding_model", "comment"):
        assert f"{name} = 'Bob\\'s \\\\ notes\\\\'" in sql


@pytest.mark.parametrize("existing,full_refresh", [(False, False), (True, True)])
def test_creation_bypasses_drift(context, render, existing, full_refresh):
    build = render(
        MATERIALIZATION,
        "cortex_search_service_get_build",
        SERVICE if existing else None,
        SERVICE,
        full_refresh,
        "fail",
    )
    assert (
        build["sql"]
        .strip()
        .startswith(
            "create or replace" if full_refresh else "create cortex search service if not exists"
        )
    )
    assert build["full_refresh_mode"] is full_refresh
    context["snowflake__describe_cortex_search_service"].assert_not_called()


@pytest.mark.parametrize("change", ["unchanged", "alter", "replace"])
@pytest.mark.parametrize("policy", ["apply", "continue", "fail"])
def test_configuration_policy(config_values, context, render, change, policy):
    if change != "unchanged":
        config_values["target_lag"] = "2 hours"
    if change == "replace":
        config_values["attributes"] = ["doc_id"]
    if policy == "fail" and change != "unchanged":
        with pytest.raises(CompilationError, match="configuration changed.*fail"):
            render(
                MATERIALIZATION, "cortex_search_service_get_build", SERVICE, SERVICE, False, policy
            )
        return
    build = render(
        MATERIALIZATION, "cortex_search_service_get_build", SERVICE, SERVICE, False, policy
    )
    replacing = policy == "apply" and change == "replace"
    assert build["full_refresh_mode"] is replacing
    if change == "unchanged" or policy == "continue":
        assert build["sql"] == ""
    else:
        assert (
            build["sql"]
            .strip()
            .startswith("create or replace" if replacing else "alter cortex search service")
        )
    assert context["exceptions"].warn.call_count == (
        policy == "continue" and change != "unchanged"
    )


@pytest.mark.parametrize("build_sql,replaced", [("", False), ("create service", True)])
def test_materialization_hooks_grants_and_result(
    config_values, context, render, build_sql, replaced
):
    build = {"sql": build_sql, "full_refresh_mode": replaced}
    config_values["grants"] = {"usage": ["reader"]}
    context.update(this=SERVICE, pre_hooks=["pre"], post_hooks=["post"])
    for name in (
        "set_query_tag",
        "unset_query_tag",
        "run_hooks",
        "apply_grants",
        "should_revoke",
        "cortex_search_service_execute_no_op",
        "cortex_search_service_execute_build_sql",
    ):
        context[name] = mock.Mock(return_value="")
    context["should_revoke"].return_value = False
    context["should_full_refresh"] = lambda: False
    context["cortex_search_service_get_build"] = mock.Mock(return_value=build)
    context["snowflake__get_cortex_search_service"] = mock.Mock(return_value=SERVICE)
    context["run_hooks"].side_effect = (
        lambda hooks: context["apply_grants"].assert_not_called() or ""
    )
    result = render(MATERIALIZATION, "dbt_macro__materialization_cortex_search_service_snowflake")
    (target_relation,) = result["relations"]
    assert target_relation.render() == SERVICE.render()
    assert target_relation.is_cortex_search_service
    context["snowflake__get_cortex_search_service"].assert_called_once_with(target_relation)
    assert context["run_hooks"].call_args_list == [mock.call(["pre"]), mock.call(["post"])]
    context["should_revoke"].assert_called_once_with(SERVICE, full_refresh_mode=replaced)
    context["apply_grants"].assert_called_once_with(
        target_relation, config_values["grants"], should_revoke=False
    )
    active = (
        "cortex_search_service_execute_build_sql"
        if build_sql
        else "cortex_search_service_execute_no_op"
    )
    context[active].assert_called_once_with(build_sql if build_sql else target_relation)


@pytest.mark.parametrize(
    "name,args,expected",
    [
        ("show_grant", (), "show grants on cortex search service db.sch.search"),
        (
            "grant",
            ("usage", ["r1", "r2"]),
            "grant usage on cortex search service db.sch.search to r1, r2",
        ),
        (
            "revoke",
            ("usage", ["r1"]),
            'revoke usage on cortex search service db.sch.search from "r1"',
        ),
    ],
)
def test_grant_namespace_and_fallback(context, render, name, args, expected):
    assert render("apply_grants.sql", f"snowflake__get_{name}_sql", SERVICE, *args) == expected
    fallback = mock.Mock(return_value="default SQL")
    context[f"default__get_{name}_sql"] = fallback
    table = SERVICE.incorporate(type=SnowflakeRelation.Table)
    assert render("apply_grants.sql", f"snowflake__get_{name}_sql", table, *args) == "default SQL"
    fallback.assert_called_once_with(table, *args)


@pytest.mark.parametrize("indexing_error", [None, "Change tracking is not enabled on DOCS"])
def test_describe_service_configuration(config_values, context, render, indexing_error):
    context["run_query"] = mock.Mock(
        return_value=agate.Table(
            [["BODY", "REGION", "WH", "1 hour", indexing_error]],
            ["SEARCH_COLUMN", "ATTRIBUTE_COLUMNS", "WAREHOUSE", "TARGET_LAG", "INDEXING_ERROR"],
            column_types=[agate.Text()] * 5,
        )
    )
    context["snowflake__describe_cortex_search_service"] = lambda relation: render(
        "relations/cortex_search/describe.sql",
        "snowflake__describe_cortex_search_service",
        relation,
    )
    config_values["target_lag"] = "2 hours"
    if indexing_error:
        with pytest.raises(
            CompilationError, match="indexing error.*change tracking.*--full-refresh"
        ):
            render(
                MATERIALIZATION,
                "cortex_search_service_get_build",
                SERVICE,
                SERVICE,
                False,
                "apply",
            )
    else:
        build = render(
            MATERIALIZATION, "cortex_search_service_get_build", SERVICE, SERVICE, False, "apply"
        )
        assert " ".join(build["sql"].split()) == (
            "alter cortex search service db.sch.search set target_lag = '2 hours'"
        )
    assert " ".join(context["run_query"].call_args.args[0].split()) == (
        "describe cortex search service db.sch.search"
    )


@pytest.mark.parametrize("rows", [[], [["BODY"], ["BODY"]]])
def test_describe_rejects_missing_or_ambiguous_metadata(context, render, rows):
    context["run_query"] = mock.Mock(
        return_value=agate.Table(rows, ["SEARCH_COLUMN"], column_types=[agate.Text()])
    )
    with pytest.raises(
        CompilationError, match="Could not read Cortex Search service configuration"
    ):
        render(
            "relations/cortex_search/describe.sql",
            "snowflake__describe_cortex_search_service",
            SERVICE,
        )


@pytest.mark.parametrize(
    "identifier,quoted,names,expected",
    [
        ("support_search", False, ["SUPPORT_SEARCH"], "SUPPORT_SEARCH"),
        ("SupportSearch", True, ["SupportSearch"], "SupportSearch"),
        ("SupportSearch", True, ["SUPPORTSEARCH"], None),
        ("support_search", False, ["SUPPORTXSEARCH"], None),
        ("support_search", False, [], None),
        ("support_search", False, ["SUPPORTXSEARCH", "SUPPORT_SEARCH"], "SUPPORT_SEARCH"),
        ("Owner's Search", True, ["Owner's Search"], "Owner's Search"),
    ],
)
def test_service_discovery(context, render, identifier, quoted, names, expected):
    relation = SnowflakeRelation.create(
        database="db",
        schema="sch",
        identifier=identifier,
        quote_policy={"identifier": quoted},
    )
    context["run_query"] = mock.Mock(
        return_value=agate.Table([[name] for name in names], ["NAME"], column_types=[agate.Text()])
    )
    # A table may exist with the same name; service discovery must ignore it.
    context["adapter"].get_relation.return_value = relation.incorporate(
        type=SnowflakeRelation.Table
    )
    result = render(
        "relations/cortex_search/show.sql", "snowflake__get_cortex_search_service", relation
    )
    context["adapter"].get_relation.assert_not_called()
    context["adapter"].check_schema_exists.assert_called_once_with("DB", "SCH")
    context["run_query"].assert_called_once()
    identifier_literal = (identifier if quoted else identifier.upper()).replace("'", "''")
    assert " ".join(context["run_query"].call_args.args[0].split()) == (
        f"show cortex search services like '{identifier_literal}' in schema DB.SCH"
    )
    if expected is None:
        assert result is None
    else:
        assert result.identifier == expected
        assert result.is_cortex_search_service


def test_service_discovery_skips_missing_schema(context, render):
    context["adapter"].check_schema_exists.return_value = False
    context["run_query"] = mock.Mock()
    assert (
        render("relations/cortex_search/show.sql", "snowflake__get_cortex_search_service", SERVICE)
        is None
    )
    context["run_query"].assert_not_called()


def test_drop_namespace(context, render):
    service = SERVICE.incorporate(
        path={"identifier": "Owner's Search"}, quote_policy={"identifier": True}
    )
    context["snowflake__get_drop_cortex_search_service_sql"] = lambda relation: render(
        "relations/cortex_search/drop.sql",
        "snowflake__get_drop_cortex_search_service_sql",
        relation,
    )
    assert (
        render("relations/drop.sql", "snowflake__get_drop_sql", service)
        == 'drop cortex search service if exists db.sch."Owner\'s Search"'
    )
