from types import SimpleNamespace
from unittest.mock import patch

import agate
import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs.interactive_table import (
    INTERACTIVE_TABLE_COLUMNS,
    SnowflakeInteractiveTableClusterByConfigChange,
    SnowflakeInteractiveTableConfig,
    SnowflakeInteractiveTableConfigChangeset,
    SnowflakeInteractiveTableRefreshWarehouseConfigChange,
    SnowflakeInteractiveTableTargetLagConfigChange,
    _absent_to_none,
    _normalize_cluster_by,
    _normalize_target_lag,
    _normalize_warehouse,
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
    # Force Text typing: agate's default type inference varies per column with whichever
    # fields a test overrides, which production never hits because
    # normalize_show_objects_result always forces these columns to Text first.
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
    """Both sides are transformed by normalization (parens stripped, casefolded) yet the
    key lists genuinely differ -- normalization must not over-collapse."""
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


# --- LINEAR prefix tolerance ---


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


# --- Quote-awareness: quoted identifiers are case-SENSITIVE in Snowflake ---


def test_case_is_preserved_inside_double_quotes():
    assert _normalize_cluster_by('("MixedCase")') == '"MixedCase"'


def test_case_is_folded_outside_double_quotes():
    assert _normalize_cluster_by("(ID, VAL)") == _normalize_cluster_by("id, val")


def test_quoted_mixed_case_readback_is_a_genuine_change():
    """`"MixedCase"` and `"mixedcase"` are different columns, so a config of
    one against a readback of the other must not compare equal."""
    assert _normalize_cluster_by('"mixedcase"') != _normalize_cluster_by('("MixedCase")')


def test_quoted_same_case_readback_is_not_a_change():
    assert _normalize_cluster_by('"MixedCase"') == _normalize_cluster_by('("MixedCase")')


def test_quoted_comma_is_not_a_key_separator():
    assert _normalize_cluster_by('("a,b")') == '"a,b"'


def test_quoted_paren_does_not_confuse_outer_paren_balance():
    assert _normalize_cluster_by('("c(d)")') == '"c(d)"'


def test_quoted_multi_key_list():
    assert _normalize_cluster_by('("MixedCase", id)') == '"MixedCase", id'


def test_doubled_quote_escape_stays_inside_the_identifier():
    """`""` inside a quoted identifier is an escaped literal quote, not the end
    of the identifier -- the embedded comma must not split the key."""
    assert _normalize_cluster_by('("a""b,c")') == '"a""b,c"'


# --- Quote-awareness: warehouse identifiers -------------------------------
# Unlike cluster_by, `SHOW` never echoes back the quote delimiters for a
# warehouse name -- only the resolved name -- so a quoted config value must
# have its delimiters stripped before folding, or it never compares equal to
# its own readback.


def test_unquoted_warehouse_is_folded():
    assert _normalize_warehouse("MY_WH") == _normalize_warehouse("my_wh")


def test_quoted_warehouse_strips_delimiters_before_folding():
    assert _normalize_warehouse('"Init_WH"') == "init_wh"


def test_quoted_warehouse_matches_its_own_unquoted_readback():
    assert _normalize_warehouse('"Init_WH"') == _normalize_warehouse("Init_WH")


def test_doubled_quote_escape_stays_inside_the_warehouse_name():
    assert _normalize_warehouse('"a""b"') == 'a"b'


@pytest.mark.parametrize(
    "configured,returned",
    [("60 seconds", "1 minute"), ("120 seconds", "2 minutes"), ("1 hour", "1 hour")],
)
def test_canonicalized_target_lag_readback_is_not_a_change(configured, returned):
    desired = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(target_lag=configured, snowflake_warehouse="wh")
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
    """These are readback spellings of ABSENCE -- normalize at LOAD time so the alter
    macro's falsy-keyed `unset` branch works."""
    existing = SnowflakeInteractiveTableConfig.from_relation_results(
        readback(initialization_warehouse=sentinel)
    )
    assert existing.snowflake_initialization_warehouse is None


def test_static_interactive_table_has_no_target_lag_or_warehouse():
    existing = SnowflakeInteractiveTableConfig.from_relation_results(readback())
    assert existing.target_lag is None
    assert existing.refresh_warehouse is None


def test_absent_to_none_collapses_sentinels_without_casefolding():
    """Casefolding is owned by `_normalize_warehouse`; doing it here would diverge the
    stored value from what Snowflake reported."""
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


def test_identical_config_produces_no_changes():
    """The phantom-diff guard, end to end: readback formatting differs from the
    configured text in every field, yet nothing changed."""
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
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="WH"),
        model_config(target_lag=None),
    )
    assert changeset.target_lag.action == RelationConfigChangeAction.drop
    assert changeset.requires_full_refresh is True


def test_builder_marks_target_lag_addition_as_create():
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag=None),
        model_config(target_lag="1 hour", refresh_warehouse="WH"),
    )
    assert changeset.target_lag.action == RelationConfigChangeAction.create
    assert changeset.requires_full_refresh is True


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
    """Snowflake requires WAREHOUSE whenever TARGET_LAG is set, so a table configured
    with only `snowflake_warehouse` still reads back a real refresh_warehouse --
    comparing that against a None desired value would be a phantom diff every run."""
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="ANALYTICS_WH"),
        model_config(target_lag="1 hour", snowflake_warehouse="analytics_wh"),
    )
    assert changeset is None


def test_builder_detects_genuine_warehouse_change_with_raw_context():
    """A real change is still caught, and `context` carries the raw effective
    value (not normalized/casefolded) with its original casing."""
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="OLD_WH"),
        model_config(target_lag="1 hour", snowflake_warehouse="New_Wh"),
    )
    assert changeset.refresh_warehouse.context == "New_Wh"


def test_static_snowflake_initialization_warehouse_only_produces_no_phantom_diff():
    """INITIALIZATION_WAREHOUSE is only accepted when TARGET_LAG is set, so a static
    table always reads back NULL -- a project-wide config must not diff against it."""
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(),
        model_config(snowflake_initialization_warehouse="analytics_wh"),
    )
    assert changeset is None


def test_static_snowflake_warehouse_only_produces_no_phantom_diff():
    """WAREHOUSE is only accepted when TARGET_LAG is set, so a static table always reads
    back NULL -- `warehouse_parameter`'s fallback must not diff against it."""
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(),
        model_config(snowflake_warehouse="analytics_wh"),
    )
    assert changeset is None


def test_builder_classifies_literal_none_string_target_lag_as_drop():
    """A user writing `target_lag: 'none'` puts the literal string into the config side
    raw, while agate coerces the readback side to a true None. The action-selection
    branch must therefore test `target_lag_normalized`, or this dynamic->static
    transition is misclassified as `alter` and Snowflake rejects it (001422)."""
    changeset = SnowflakeRelation.interactive_table_config_changeset(
        readback(target_lag="1 hour", refresh_warehouse="WH"),
        model_config(target_lag="none"),
    )
    assert changeset.target_lag.action == RelationConfigChangeAction.drop
    assert changeset.requires_full_refresh is True


def test_is_dynamic_is_false_for_literal_none_string_target_lag():
    """`is_dynamic` must agree with `target_lag_normalized`, the same signal the
    changeset builder uses to classify transitions -- `'none'` is a spelling of
    absence, so `is_dynamic` is False despite `target_lag` being a non-None string."""
    config = SnowflakeInteractiveTableConfig.from_relation_config(model_config(target_lag="none"))
    assert config.is_dynamic is False


def test_is_dynamic_is_true_for_a_real_target_lag():
    config = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(target_lag="1 hour", snowflake_warehouse="wh")
    )
    assert config.is_dynamic is True


def test_missing_cluster_by_raises():
    relation_config = model_config(cluster_by=None)
    with pytest.raises(CompilationError, match="cluster_by"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


@pytest.mark.parametrize("cluster_by", [[], "", "   ", ["  "], ["", ""]])
def test_wholly_blank_cluster_by_raises(cluster_by):
    relation_config = model_config(cluster_by=cluster_by)
    with pytest.raises(CompilationError, match="cluster_by"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


@pytest.mark.parametrize("cluster_by", [["id", "  "], ["  ", "id"], ["id", "", "val"]])
def test_cluster_by_list_with_a_blank_entry_raises(cluster_by):
    """The joined string is truthy, so a blank ELEMENT slipped through and
    rendered invalid DDL (`cluster by (id,   )`, Snowflake 001003)."""
    relation_config = model_config(cluster_by=cluster_by)
    with pytest.raises(CompilationError, match="cluster_by"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_iceberg_table_format_raises():
    relation_config = model_config(table_format="iceberg")
    with pytest.raises(CompilationError, match="iceberg"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_transient_raises():
    relation_config = model_config(transient=True)
    with pytest.raises(CompilationError, match="transient"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_target_lag_without_warehouse_raises():
    relation_config = model_config(target_lag="1 hour")
    with pytest.raises(CompilationError, match="warehouse"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


# --- blank/whitespace-only warehouse values are unset, not valid names ---


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
@pytest.mark.parametrize("field", ["snowflake_warehouse", "refresh_warehouse"])
def test_target_lag_with_a_blank_warehouse_raises(field, blank):
    """Python truthiness treats `"  "` as a real name, so a whitespace-only value
    slipped past validation and rendered `warehouse =   `."""
    relation_config = model_config(target_lag="1 hour", **{field: blank})
    with pytest.raises(CompilationError, match="warehouse"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_blank_refresh_warehouse_falls_back_to_snowflake_warehouse(blank):
    config = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(target_lag="1 hour", refresh_warehouse=blank, snowflake_warehouse="REAL_WH")
    )

    assert config.warehouse_parameter == "REAL_WH"


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_wholly_blank_warehouses_yield_no_warehouse_parameter(blank):
    config = SnowflakeInteractiveTableConfig.from_dict(
        {
            "name": "tbl",
            "schema_name": "sch",
            "database_name": "db",
            "cluster_by": "id",
            "refresh_warehouse": blank,
            "snowflake_warehouse": blank,
        }
    )

    assert config.warehouse_parameter is None


@pytest.mark.parametrize("cleared", ["NONE", "none", "", "  "])
def test_clearing_init_warehouse_by_literal_is_stored_as_none(cleared):
    """A config-side clear must collapse to None so the alter macro emits
    `unset initialization_warehouse` instead of `set ... = NONE`."""
    config = SnowflakeInteractiveTableConfig.from_relation_config(
        model_config(
            target_lag="1 hour",
            snowflake_warehouse="wh",
            snowflake_initialization_warehouse=cleared,
        )
    )

    assert config.snowflake_initialization_warehouse is None


def test_target_lag_with_warehouse_does_not_raise():
    relation_config = model_config(target_lag="1 hour", snowflake_warehouse="wh")
    SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)  # should not raise


# --- inert snowflake_initialization_warehouse warning ---


@pytest.mark.parametrize("target_lag", [None, "none", "NONE", "", "   "])
def test_init_warehouse_without_target_lag_warns(target_lag):
    """A static interactive table can't hold a warehouse, so the value is dropped
    before any DDL. Inert rather than fatal, so warn instead of erroring."""
    relation_config = model_config(
        target_lag=target_lag, snowflake_initialization_warehouse="INIT_WH"
    )

    with patch(
        "dbt.adapters.snowflake.relation_configs.interactive_table.warn_or_error"
    ) as mock_warn:
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)

    assert mock_warn.call_count == 1
    assert "snowflake_initialization_warehouse is ignored" in mock_warn.call_args.args[0].base_msg


def test_init_warehouse_with_target_lag_does_not_warn():
    relation_config = model_config(
        target_lag="1 hour",
        snowflake_warehouse="wh",
        snowflake_initialization_warehouse="INIT_WH",
    )

    with patch(
        "dbt.adapters.snowflake.relation_configs.interactive_table.warn_or_error"
    ) as mock_warn:
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)

    mock_warn.assert_not_called()


def test_no_init_warehouse_without_target_lag_does_not_warn():
    relation_config = model_config(target_lag=None)

    with patch(
        "dbt.adapters.snowflake.relation_configs.interactive_table.warn_or_error"
    ) as mock_warn:
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)

    mock_warn.assert_not_called()


def test_snowflake_warehouse_without_target_lag_does_not_warn():
    """DIVERGES from the init-warehouse case on purpose: `snowflake_warehouse`
    still selects the warehouse the build runs on, so it isn't inert."""
    relation_config = model_config(target_lag=None, snowflake_warehouse="wh")

    with patch(
        "dbt.adapters.snowflake.relation_configs.interactive_table.warn_or_error"
    ) as mock_warn:
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)

    mock_warn.assert_not_called()


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
