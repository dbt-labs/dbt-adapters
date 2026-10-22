from types import SimpleNamespace

import pytest
from dbt.contracts.graph.model_config import ModelConfig
from dbt_common.exceptions import CompilationError

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeCortexSearchConfig


def search_config(**overrides):
    values = {
        "materialized": "cortex_search_service",
        "search_column": '"Body Text"',
        "attributes": ['"Region Name"', "doc_id"],
        "target_lag": "1 hour",
        **overrides,
    }
    model = SimpleNamespace(config=ModelConfig.from_dict(values))
    return SnowflakeRelation.from_config(model)


def test_model_config_preserves_options_and_normalizes_enums():
    search = search_config(
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
    assert search.refresh_mode == "INCREMENTAL"
    assert search.initialize == "ON_SCHEDULE"
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
