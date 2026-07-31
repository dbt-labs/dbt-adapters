from types import SimpleNamespace

import agate
import pytest

from dbt.adapters.snowflake.relation_configs.interactive_table import (
    INTERACTIVE_TABLE_COLUMNS,
    SnowflakeInteractiveTableConfig,
)


def model_config(**overrides):
    """Builds a MODEL CONFIG (what the user wrote). Never used as readback."""
    config = {
        "cluster_by": ["id"],
        "target_lag": None,
        "snowflake_warehouse": None,
        "refresh_warehouse": None,
        "snowflake_initialization_warehouse": None,
    }
    config.update(overrides)
    return SimpleNamespace(
        identifier="tbl",
        schema="sch",
        database="db",
        compiled_code="select 1 as id",
        config=SimpleNamespace(extra=config, get=config.get, **{}),
    )


def readback(**overrides):
    """Builds a SHOW INTERACTIVE TABLES row. Column names and value spellings
    must match what Snowflake actually returns -- NOT what we sent.

    Columns come from INTERACTIVE_TABLE_COLUMNS so this fixture cannot drift from
    the production select list. Defaults are the STATIC interactive table shape:
    cluster_by always populated and parenthesized, everything else NULL.
    """
    row = {column: None for column in INTERACTIVE_TABLE_COLUMNS}
    row["name"] = "tbl"
    row["schema_name"] = "sch"
    row["database_name"] = "db"
    row["cluster_by"] = "(id)"  # always populated, parenthesized on readback
    unknown = set(overrides) - set(INTERACTIVE_TABLE_COLUMNS)
    assert not unknown, f"not real readback columns: {sorted(unknown)}"
    row.update(overrides)
    keys = list(INTERACTIVE_TABLE_COLUMNS)
    # column_types is MANDATORY -- see the note in Task 2's fixture. Here it matters
    # even more: most default values are None, so agate would infer a DIFFERENT type
    # per column depending on which fields a given test overrides, making behavior
    # vary between tests in the same file. Production forces Text on all of these
    # (normalize_show_objects_result defaults every unlisted column to agate.Text()).
    table = agate.Table([[row[k] for k in keys]], keys, [agate.Text()] * len(keys))
    return {"interactive_table": table}


def test_readback_fixture_uses_the_settled_column_set():
    """Guard against fixture/production drift. Column reads are tolerant, so a
    misnamed column would read as unset and every other test would still pass."""
    assert INTERACTIVE_TABLE_COLUMNS == (
        "name",
        "schema_name",
        "database_name",
        "text",
        "target_lag",
        "refresh_warehouse",
        "initialization_warehouse",
        "cluster_by",
    )
    assert list(readback()["interactive_table"].column_names) == list(INTERACTIVE_TABLE_COLUMNS)


def test_every_readback_column_round_trips_by_value():
    """Not `is_ok()`: assert the VALUES, or a wrong column name reads as None and
    passes silently."""
    existing = SnowflakeInteractiveTableConfig.from_relation_results(
        readback(
            target_lag="1 hour",
            refresh_warehouse="MY_WH",
            initialization_warehouse="INIT_WH",
            cluster_by="(id, name)",
        )
    )
    assert existing.name == "tbl"
    assert existing.schema_name == "sch"
    assert existing.database_name == "db"
    assert existing.target_lag == "1 hour"
    assert existing.refresh_warehouse == "MY_WH"
    assert existing.snowflake_initialization_warehouse == "init_wh"
    assert existing.cluster_by == "(id, name)"


def test_parenthesized_cluster_by_readback_is_not_a_change():
    """Config says 'id'; Snowflake returns '(id)'. These are the SAME."""
    desired = SnowflakeInteractiveTableConfig.from_relation_config(model_config(cluster_by=["id"]))
    existing = SnowflakeInteractiveTableConfig.from_relation_results(readback())
    assert desired.cluster_by_normalized == existing.cluster_by_normalized


def test_multi_column_parenthesized_cluster_by_readback_is_not_a_change():
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(cluster_by=["id", "name"])
    )
    existing = SnowflakeInteractiveTableConfig.from_relation_results(
        readback(cluster_by="(id, name)")
    )
    assert desired.cluster_by_normalized == existing.cluster_by_normalized


def test_cluster_by_stored_value_keeps_exact_text_for_ddl():
    """Normalization must NOT overwrite the stored value -- DDL needs it verbatim."""
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(cluster_by=["id", "name"])
    )
    assert desired.cluster_by == "id, name"


def test_a_real_cluster_by_change_is_still_detected():
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(cluster_by=["id", "name"])
    )
    existing = SnowflakeInteractiveTableConfig.from_relation_results(readback(cluster_by="(id)"))
    assert desired.cluster_by_normalized != existing.cluster_by_normalized


@pytest.mark.parametrize(
    "configured,returned",
    [("60 seconds", "1 minute"), ("120 seconds", "2 minutes"), ("1 hour", "1 hour")],
)
def test_canonicalized_target_lag_readback_is_not_a_change(configured, returned):
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(target_lag=configured)
    )
    existing = SnowflakeInteractiveTableConfig.from_relation_results(readback(target_lag=returned))
    assert desired.target_lag_normalized == existing.target_lag_normalized


def test_warehouse_case_difference_is_not_a_change():
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(target_lag="1 hour", refresh_warehouse="analytics_wh")
    )
    existing = SnowflakeInteractiveTableConfig.from_relation_results(
        readback(target_lag="1 hour", refresh_warehouse="ANALYTICS_WH")
    )
    assert desired.refresh_warehouse_normalized == existing.refresh_warehouse_normalized


@pytest.mark.parametrize("sentinel", ["", "NONE", "None", None])
def test_initialization_warehouse_absence_spellings_parse_to_none(sentinel):
    """These are readback spellings of ABSENCE -- normalize at LOAD time so the
    Phase 2 alter macro's falsy-keyed `unset` branch works."""
    existing = SnowflakeInteractiveTableConfig.from_relation_results(
        readback(initialization_warehouse=sentinel)
    )
    assert existing.snowflake_initialization_warehouse is None


def test_static_interactive_table_has_no_target_lag_or_warehouse():
    existing = SnowflakeInteractiveTableConfig.from_relation_results(readback())
    assert existing.target_lag is None
    assert existing.refresh_warehouse is None
