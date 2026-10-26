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
    assert search.target_lag == "1 hour"
    assert search.primary_key == ["doc_id"]
    assert search.embedding_model == "snowflake-arctic-embed-l-v2.0"
    assert search.refresh_mode is RefreshMode.INCREMENTAL
    assert search.initialize is Initialize.ON_SCHEDULE
    assert search.full_index_build_interval_days == 7
    assert search.request_logging is False
    assert search.auto_suspend == 1800
    assert search.comment == ""
    assert search.warehouse_parameter("PROFILE_WH") == '"Search Warehouse"'


def test_unset_optional_options_preserve_snowflake_defaults():
    search = search_config()
    for name in (
        "primary_key",
        "embedding_model",
        "refresh_mode",
        "initialize",
        "full_index_build_interval_days",
        "request_logging",
        "auto_suspend",
        "comment",
    ):
        assert getattr(search, name) is None


@pytest.mark.parametrize("warehouse", [None, ""])
def test_warehouse_falls_back_to_profile(warehouse):
    search = search_config(snowflake_warehouse=warehouse)
    assert search.warehouse_parameter('"Profile Warehouse"') == '"Profile Warehouse"'


@pytest.mark.parametrize("warehouse", [None, " ", 123])
def test_invalid_profile_warehouse_raises_compiler_error(warehouse):
    with pytest.raises(CompilationError, match="snowflake_warehouse"):
        search_config().warehouse_parameter(warehouse)


def test_missing_model_config_raises_compiler_error():
    with pytest.raises(CompilationError, match="search_column"):
        SnowflakeRelation.from_config(
            SimpleNamespace(
                config=SimpleNamespace(materialized="cortex_search_service", get=lambda name: None)
            )
        )


def test_enum_defaults_match_cortex_search_defaults():
    assert RefreshMode.default() is RefreshMode.INCREMENTAL
    assert Initialize.default() is Initialize.ON_CREATE


@pytest.mark.parametrize("mode", [RefreshMode.FULL, RefreshMode.INCREMENTAL])
def test_enum_refresh_mode_is_accepted(mode):
    assert search_config(refresh_mode=mode).refresh_mode is mode


@pytest.mark.parametrize("initialize", [Initialize.ON_CREATE, Initialize.ON_SCHEDULE])
def test_enum_initialize_is_accepted(initialize):
    assert search_config(initialize=initialize).initialize is initialize


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
        ("full_index_build_interval_days", 1.5),
        ("full_index_build_interval_days", True),
        ("full_index_build_interval_days", "7"),
        ("auto_suspend", 1799),
        ("auto_suspend", 1800.5),
        ("auto_suspend", True),
        ("auto_suspend", "1800"),
        ("request_logging", "false"),
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


def configured_search(**overrides):
    return search_config(**{"primary_key": ["doc_id"], "snowflake_warehouse": "WH", **overrides})


def test_readback_and_unset_optional_defaults_do_not_drift():
    assert configured_search().configuration_changes(metadata(), "PROFILE_WH") == {
        "alter": {},
        "replace": [],
    }


def test_query_definition_is_not_configuration_drift():
    assert configured_search().configuration_changes(
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
    changes = configured_search(**{name: value}).configuration_changes(metadata(), "WH")
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
    assert configured_search(**{name: value}).configuration_changes(metadata(), "WH") == {
        "alter": {},
        "replace": [name],
    }


def test_quoted_warehouse_is_case_sensitive():
    assert (
        configured_search(snowflake_warehouse='"Wh"').configuration_changes(
            metadata(warehouse="Wh"), "WH"
        )["alter"]
        == {}
    )
    assert configured_search(snowflake_warehouse='"Wh"').configuration_changes(metadata(), "WH")[
        "alter"
    ] == {"warehouse": '"Wh"'}


def test_missing_readback_cannot_silently_ignore_an_explicit_setting():
    current = metadata()
    current.pop("request_logging")
    with pytest.raises(DbtRuntimeError, match="Could not read.*request_logging"):
        configured_search(request_logging=False).configuration_changes(current, "WH")


def test_initialize_is_creation_only():
    assert configured_search(initialize="ON_SCHEDULE").configuration_changes(metadata(), "WH") == {
        "alter": {},
        "replace": [],
    }
