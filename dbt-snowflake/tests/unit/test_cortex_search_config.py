from types import SimpleNamespace

import pytest
from dbt.contracts.graph.model_config import ModelConfig
from dbt_common.exceptions import CompilationError, DbtRuntimeError

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeCortexSearchConfig
from dbt.adapters.snowflake.relation_configs.cortex_search import Initialize, RefreshMode


def search_config(**overrides):
    values = {
        "materialized": "cortex_search_service",
        "search_column": "body",
        "attributes": ["region"],
        "target_lag": "1 hour",
        **overrides,
    }
    model = SimpleNamespace(config=ModelConfig.from_dict(values))
    return SnowflakeRelation.from_config(model)


def test_model_config_preserves_options_and_normalizes_enums():
    search = search_config(
        search_column='"Body Text"',
        attributes=['"Region Name"', "doc_id"],
        snowflake_warehouse='"Search Warehouse"',
        primary_key=["doc_id"],
        embedding_model="snowflake-arctic-embed-l-v2.0",
        refresh_mode=" incremental ",
        initialize=" on_schedule ",
        full_index_build_interval_days=7,
        request_logging=False,
        auto_suspend=1800,
        comment="",
    )
    assert isinstance(search, SnowflakeCortexSearchConfig)
    assert search.search_column == '"Body Text"'
    assert search.attributes == ['"Region Name"', "doc_id"]
    assert search.refresh_mode is RefreshMode.INCREMENTAL
    assert search.initialize is Initialize.ON_SCHEDULE
    assert search.request_logging is False
    assert search.comment == ""
    assert search.warehouse_parameter("PROFILE_WH") == '"Search Warehouse"'


@pytest.mark.parametrize("warehouse", [None, ""])
def test_warehouse_falls_back_to_profile(warehouse):
    search = search_config(snowflake_warehouse=warehouse)
    assert search.warehouse_parameter('"Profile Warehouse"') == '"Profile Warehouse"'


@pytest.mark.parametrize("warehouse", [None, " ", 123])
def test_invalid_profile_warehouse_raises_compiler_error(warehouse):
    with pytest.raises(CompilationError, match="snowflake_warehouse"):
        search_config().warehouse_parameter(warehouse)


@pytest.mark.parametrize(
    "key,value",
    [
        ("search_column", None),
        ("search_column", " "),
        ("target_lag", 60),
        ("snowflake_warehouse", " "),
        ("attributes", "region"),
        ("attributes", []),
        ("attributes", [""]),
        ("primary_key", "doc_id"),
        ("primary_key", [""]),
        ("embedding_model", ""),
        ("refresh_mode", "AUTO"),
        ("initialize", "IMMEDIATELY"),
        ("full_index_build_interval_days", 0),
        ("full_index_build_interval_days", True),
        ("auto_suspend", 1799),
        ("auto_suspend", 1800.5),
        ("request_logging", 0),
        ("comment", 123),
    ],
)
def test_invalid_service_config(key, value):
    values = {
        "search_column": "body",
        "attributes": ["region"],
        "target_lag": "1 hour",
        key: value,
    }
    with pytest.raises(CompilationError, match=key):
        search = SnowflakeCortexSearchConfig.from_dict(values)
        search.warehouse_parameter("WH")


@pytest.mark.parametrize("primary_key", [None, []])
def test_index_rebuild_interval_requires_primary_key(primary_key):
    with pytest.raises(CompilationError, match="requires a non-empty `primary_key`"):
        search_config(primary_key=primary_key, full_index_build_interval_days=7)


def metadata(**overrides):
    return {
        "definition": "select doc_id, body, region from db.sch.docs",
        "search_column": "BODY",
        "attribute_columns": "REGION",
        "primary_key_columns": "DOC_ID",
        "warehouse": "WH",
        "target_lag": "60 minutes",
        "embedding_model": "snowflake-arctic-embed-m-v1.5",
        "refresh_mode": "INCREMENTAL",
        "request_logging": "false",
        "auto_suspend": "1800",
        "full_index_build_interval_days": "7",
        "comment": "Support articles",
        **overrides,
    }


def test_omitted_options_query_and_initialize_do_not_drift():
    assert search_config(initialize="ON_SCHEDULE").configuration_changes(
        metadata(definition="select upper(body) from docs"), "WH"
    ) == {
        "alter": {},
        "replace": [],
    }


@pytest.mark.parametrize(
    "name,value,expected",
    [
        ("target_lag", "2 hours", "target_lag"),
        ("snowflake_warehouse", "OTHER_WH", "warehouse"),
        ("comment", "", "comment"),
        ("request_logging", True, "request_logging"),
        ("auto_suspend", 3600, "auto_suspend"),
        ("full_index_build_interval_days", 3, "full_index_build_interval_days"),
    ],
)
def test_supported_scalar_changes_are_alterable(name, value, expected):
    changes = search_config(primary_key=["doc_id"], **{name: value}).configuration_changes(
        metadata(), "WH"
    )
    assert changes == {"alter": {expected: value}, "replace": []}


@pytest.mark.parametrize(
    "name,value",
    [
        ("search_column", "doc_id"),
        ("attributes", ["doc_id"]),
        ("primary_key", ["body"]),
        ("embedding_model", "snowflake-arctic-embed-l-v2.0"),
        ("refresh_mode", "FULL"),
    ],
)
def test_index_configuration_changes_require_replacement(name, value):
    assert search_config(**{name: value}).configuration_changes(metadata(), "WH") == {
        "alter": {},
        "replace": [name],
    }


def test_quoted_warehouse_is_case_sensitive():
    assert (
        search_config(snowflake_warehouse='"Wh"').configuration_changes(
            metadata(warehouse="Wh"), "WH"
        )["alter"]
        == {}
    )
    assert search_config(snowflake_warehouse='"Wh"').configuration_changes(metadata(), "WH")[
        "alter"
    ] == {"warehouse": '"Wh"'}


def test_missing_readback_cannot_silently_ignore_an_explicit_setting():
    current = metadata()
    current.pop("request_logging")
    with pytest.raises(DbtRuntimeError, match="Could not read.*request_logging"):
        search_config(request_logging=False).configuration_changes(current, "WH")
