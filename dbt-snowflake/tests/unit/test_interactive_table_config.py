from types import SimpleNamespace

import agate
import pytest

from dbt.adapters.snowflake.relation_configs.interactive_table import (
    INTERACTIVE_TABLE_COLUMNS,
    SnowflakeInteractiveTableConfig,
    _absent_to_none,
    _normalize_cluster_by,
    _normalize_target_lag,
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
    # Stored raw -- casefolding is a comparison concern, not a load-time one.
    assert existing.snowflake_initialization_warehouse == "INIT_WH"
    assert existing.snowflake_initialization_warehouse_normalized == "init_wh"
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
    """Non-vacuous: both sides are meaningfully transformed by normalization
    (the readback side has its outer parens stripped, and both sides are
    casefolded from upper to lower) yet the underlying key lists genuinely
    differ ('name' vs 'other') and must still compare unequal afterward. This
    proves normalization doesn't over-collapse a real difference -- comparing
    raw, untransformed strings (e.g. 'id, name' vs '(id)') would pass even if
    `_normalize_cluster_by` were replaced by the identity function, so it
    wouldn't prove anything about normalization itself."""
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(cluster_by=["ID", "NAME"])
    )
    existing = SnowflakeInteractiveTableConfig.from_relation_results(
        readback(cluster_by="(ID, OTHER)")
    )
    assert desired.cluster_by_normalized != existing.cluster_by_normalized


def test_cluster_by_with_mismatched_outer_parens_is_not_stripped():
    """`(a)` followed by `to_date(ts)` starts with `(` and ends with `)`, but the
    leading paren is closed by the one after `a`, not the trailing one -- these are
    NOT a single balanced outer pair and must be left alone."""
    assert _normalize_cluster_by("(a), to_date(ts)") == "(a), to_date(ts)"


def test_singly_and_doubly_wrapped_cluster_by_lists_normalize_the_same():
    """`(a), (b)` (two independently parenthesized keys) and `((a), (b))` (the same
    list wrapped in one more outer pair) denote the SAME clustering key list and
    must compare equal."""
    assert _normalize_cluster_by("(a), (b)") == _normalize_cluster_by("((a), (b))")


def test_single_balanced_outer_pair_still_strips():
    assert _normalize_cluster_by("(id)") == "id"


# --- LINEAR prefix tolerance (UNVERIFIED against a live warehouse -- see the
# docstring on `_normalize_cluster_by`) -----------------------------------------


def test_linear_prefixed_bare_and_parenthesized_cluster_by_normalize_the_same():
    assert (
        _normalize_cluster_by("LINEAR(ID, VAL)")
        == _normalize_cluster_by("(ID, VAL)")
        == _normalize_cluster_by("ID, VAL")
    )


def test_linear_prefix_match_is_case_insensitive():
    assert _normalize_cluster_by("linear(ID, VAL)") == _normalize_cluster_by("LINEAR(ID, VAL)")


def test_linear_named_column_alone_is_untouched():
    assert _normalize_cluster_by("linear") == "linear"


def test_parenthesized_linear_named_column_still_unwraps():
    """A column named `linear` wrapped in the ordinary outer-paren spelling
    unwraps just like any other single key -- the LINEAR-prefix handling
    must not interfere with this unrelated case."""
    assert _normalize_cluster_by("(linear)") == "linear"


def test_linear_function_call_as_one_of_several_keys_is_untouched():
    """`linear(a)` here is a clustering EXPRESSION (a call to a function named
    `linear`), not Snowflake's wrapper -- the leading group doesn't close at
    the final character because `, b` follows it."""
    assert _normalize_cluster_by("linear(a), b") == "linear(a), b"


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


def test_absent_to_none_collapses_sentinels_without_casefolding():
    """`_absent_to_none` is the LOAD-TIME helper: it only collapses the wire
    spellings of absence to None. Casefolding is a comparison concern owned
    by `_normalize_warehouse` and must NOT happen here, or the stored value
    would silently diverge from what Snowflake actually reported."""
    assert _absent_to_none("") is None
    assert _absent_to_none("none") is None
    assert _absent_to_none("NONE") is None
    assert _absent_to_none("  ") is None
    assert _absent_to_none("INIT_WH") == "INIT_WH"


def test_target_lag_fallback_collapses_internal_whitespace():
    """Unrecognized target_lag forms fall back to a casefolded string
    returned as-is; double internal whitespace must not create a phantom
    diff between two spellings of the same lag."""
    assert _normalize_target_lag("2 weeks") == _normalize_target_lag("2  weeks")


from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation_configs.interactive_table import (
    SnowflakeInteractiveTableClusterByConfigChange,
    SnowflakeInteractiveTableConfigChangeset,
    SnowflakeInteractiveTableRefreshWarehouseConfigChange,
    SnowflakeInteractiveTableTargetLagConfigChange,
)


def test_cluster_by_change_requires_full_refresh():
    """DIVERGES from dynamic tables, which return False here."""
    change = SnowflakeInteractiveTableClusterByConfigChange(
        action=RelationConfigChangeAction.alter, context="id, name"
    )
    assert change.requires_full_refresh is True


def test_target_lag_value_change_does_not_require_full_refresh():
    change = SnowflakeInteractiveTableTargetLagConfigChange(
        action=RelationConfigChangeAction.alter, context="2 hours"
    )
    assert change.requires_full_refresh is False


def test_target_lag_removal_requires_full_refresh():
    change = SnowflakeInteractiveTableTargetLagConfigChange(
        action=RelationConfigChangeAction.drop, context=None
    )
    assert change.requires_full_refresh is True


def test_target_lag_addition_requires_full_refresh():
    change = SnowflakeInteractiveTableTargetLagConfigChange(
        action=RelationConfigChangeAction.create, context="1 hour"
    )
    assert change.requires_full_refresh is True


def test_warehouse_change_does_not_require_full_refresh():
    change = SnowflakeInteractiveTableRefreshWarehouseConfigChange(
        action=RelationConfigChangeAction.alter, context="OTHER_WH"
    )
    assert change.requires_full_refresh is False


def test_changeset_aggregates_full_refresh():
    changeset = SnowflakeInteractiveTableConfigChangeset(
        target_lag=SnowflakeInteractiveTableTargetLagConfigChange(
            action=RelationConfigChangeAction.alter, context="2 hours"
        ),
    )
    assert changeset.has_changes is True
    assert changeset.requires_full_refresh is False

    changeset = SnowflakeInteractiveTableConfigChangeset(
        cluster_by=SnowflakeInteractiveTableClusterByConfigChange(
            action=RelationConfigChangeAction.alter, context="id"
        ),
    )
    assert changeset.requires_full_refresh is True


def test_empty_changeset_has_no_changes():
    assert SnowflakeInteractiveTableConfigChangeset().has_changes is False


# --- the builder: identical config must produce NO changes -------------------


def test_identical_config_produces_no_changes():
    """The phantom-diff guard, end to end: readback formatting differs from the
    configured text in every field, yet nothing changed."""
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(
            cluster_by="(id)",
            target_lag="1 minute",
            refresh_warehouse="ANALYTICS_WH",
        ),
        model_config(
            cluster_by=["id"],
            target_lag="60 seconds",
            refresh_warehouse="analytics_wh",
        ),
    )
    assert changeset is None


def test_builder_marks_target_lag_removal_as_drop():
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="WH"),
        model_config(target_lag=None),
    )
    assert changeset.target_lag.action == RelationConfigChangeAction.drop
    assert changeset.requires_full_refresh is True


def test_builder_marks_target_lag_addition_as_create():
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag=None),
        model_config(target_lag="1 hour", refresh_warehouse="WH"),
    )
    assert changeset.target_lag.action == RelationConfigChangeAction.create
    assert changeset.requires_full_refresh is True


# --- warehouse fallback (Item 1) ----------------------------------------------


def test_warehouse_parameter_falls_back_to_snowflake_warehouse():
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(snowflake_warehouse="analytics_wh")
    )
    assert desired.warehouse_parameter == "analytics_wh"


def test_refresh_warehouse_takes_precedence_over_snowflake_warehouse():
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(snowflake_warehouse="wh_a", refresh_warehouse="wh_b")
    )
    assert desired.warehouse_parameter == "wh_b"


def test_snowflake_warehouse_only_produces_no_phantom_diff():
    """Configuring only `snowflake_warehouse` -- the ordinary dbt-snowflake way
    to say which warehouse a model uses -- must not diff against the
    readback's refresh_warehouse when Snowflake reports that same warehouse
    back. Snowflake requires WAREHOUSE whenever TARGET_LAG is set, so an
    interactive table with only snowflake_warehouse configured still reads
    back a real refresh_warehouse; comparing that against a None desired
    refresh_warehouse would be a phantom diff on every run."""
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="ANALYTICS_WH"),
        model_config(target_lag="1 hour", snowflake_warehouse="analytics_wh"),
    )
    assert changeset is None


def test_builder_detects_genuine_warehouse_change_with_raw_context():
    """A real change is still caught, and `context` carries the raw effective
    value (not normalized/casefolded) with its original casing."""
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="OLD_WH"),
        model_config(target_lag="1 hour", snowflake_warehouse="New_Wh"),
    )
    assert changeset.refresh_warehouse.context == "New_Wh"


def test_static_snowflake_initialization_warehouse_only_produces_no_phantom_diff():
    """A STATIC interactive table (no target_lag) has no initialization warehouse
    concept in Snowflake -- INITIALIZATION_WAREHOUSE is only accepted (and only
    reported back) when TARGET_LAG is set, so a static table always reads back
    `initialization_warehouse = NULL`. A project-wide
    `snowflake_initialization_warehouse` (e.g. via `models: +snowflake_initialization_warehouse:`)
    must not diff against that None readback -- mirrors
    `test_static_snowflake_warehouse_only_produces_no_phantom_diff` for
    `refresh_warehouse`."""
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(),
        model_config(snowflake_initialization_warehouse="analytics_wh"),
    )
    assert changeset is None


def test_static_snowflake_warehouse_only_produces_no_phantom_diff():
    """A STATIC interactive table (no target_lag) has no refresh warehouse in
    Snowflake -- WAREHOUSE is only accepted when TARGET_LAG is set, so a
    static table always reads back `refresh_warehouse = NULL`. A project-wide
    `snowflake_warehouse` (e.g. via `models: +snowflake_warehouse:`) must not
    diff against that None readback just because `warehouse_parameter` falls
    back to it -- that fallback is only meaningful for a dynamic table."""
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(),
        model_config(snowflake_warehouse="analytics_wh"),
    )
    assert changeset is None


# --- target_lag action/normalization consistency (Item 2) --------------------


def test_builder_classifies_literal_none_string_target_lag_as_drop():
    """The readback side can NEVER carry the literal string "NONE" --
    `agate.Text()`'s default `null_values` (`'', 'na', 'n/a', 'none', 'null',
    '.'`, case-insensitive) coerce it to a true `None` at Table-construction
    time, before any of our code runs. But `new.target_lag` comes from the
    model config's `extra.get("target_lag")` entirely raw -- no agate
    coercion applies on that side. A user writing `target_lag: 'none'` to
    make an existing dynamic table static puts the literal string "none"
    into `new.target_lag`, not a true `None`. The action-selection branch
    must test `target_lag_normalized` (which treats "none" as absent) on
    both sides, matching the gating condition, or this dynamic->static
    transition is misclassified as `alter` -- which Snowflake rejects
    (001420) instead of the `drop`-and-rebuild this requires."""
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="WH"),
        model_config(target_lag="none"),
    )
    assert changeset.target_lag.action == RelationConfigChangeAction.drop
    assert changeset.requires_full_refresh is True


# --- aggregation coverage across mixed changes (Item 3) -----------------------


def test_is_dynamic_is_false_for_literal_none_string_target_lag():
    """Item 2: `is_dynamic` must agree with `target_lag_normalized`, the same
    signal the changeset builder uses to classify transitions. `target_lag='none'`
    is the readback/config spelling of absence (see
    `test_builder_classifies_literal_none_string_target_lag_as_drop`), so
    `is_dynamic` must be False here even though `target_lag` itself is the
    non-None string `'none'`."""
    config = SnowflakeInteractiveTableConfig.from_relation_config(model_config(target_lag="none"))
    assert config.is_dynamic is False


def test_is_dynamic_is_true_for_a_real_target_lag():
    config = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(target_lag="1 hour")
    )
    assert config.is_dynamic is True


def test_changeset_aggregates_full_refresh_across_mixed_changes():
    """A single changeset containing both a full-refresh-requiring change
    (cluster_by) and a non-full-refresh one (target_lag alter) must still
    report requires_full_refresh True -- the scenario the `any()`
    aggregation exists for."""
    changeset = SnowflakeInteractiveTableConfigChangeset(
        target_lag=SnowflakeInteractiveTableTargetLagConfigChange(
            action=RelationConfigChangeAction.alter, context="2 hours"
        ),
        cluster_by=SnowflakeInteractiveTableClusterByConfigChange(
            action=RelationConfigChangeAction.alter, context="id"
        ),
    )
    assert changeset.has_changes is True
    assert changeset.requires_full_refresh is True
