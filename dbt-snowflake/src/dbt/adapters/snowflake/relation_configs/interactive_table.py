from dataclasses import dataclass
from typing import Optional

from dbt_common.exceptions import CompilationError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationResults,
)

from dbt.adapters.snowflake import parse_model
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

# Readback spellings that mean "this is not set".
_ABSENT = {"", "none"}

# The exact `SHOW INTERACTIVE TABLES` select list, in order. Settled by the v2
# track (commit d7d9437c60) -- do NOT rename `refresh_warehouse` to `warehouse`
# to match the dynamic-table code; they are different SHOW commands.
#
# ONE source of truth on purpose: the Phase 2 describe method MUST select exactly
# these columns by referencing this constant, and the test fixtures MUST build
# their rows from it. Column reads are tolerant (a missing column yields None),
# so a fixture that drifts from the production select list fails SILENTLY --
# the value reads as unset and nothing raises.
INTERACTIVE_TABLE_COLUMNS = (
    "name",
    "schema_name",
    "database_name",
    "text",
    "target_lag",
    "refresh_warehouse",
    "initialization_warehouse",
    "cluster_by",
)


def _normalize_warehouse(value: Optional[str]) -> Optional[str]:
    """Snowflake folds unquoted identifiers to upper case, so warehouse names
    must compare case-insensitively."""
    if value is None:
        return None
    stripped = value.strip()
    if stripped.casefold() in _ABSENT:
        return None
    return stripped.casefold()


def _absent_to_none(value: Optional[str]) -> Optional[str]:
    """Collapse the wire spellings of "not set" to None, at LOAD time.

    Deliberately does NOT casefold: that's a comparison concern owned by
    `_normalize_warehouse`. This is only for values that are the wire
    spelling of ABSENCE -- Snowflake reads back `''` for an unset
    initialization warehouse, and that must become `None` at load so
    absence detection works. Anything else is stored byte-faithful to what
    Snowflake reported.
    """
    if value is None:
        return None
    stripped = value.strip()
    if stripped.casefold() in _ABSENT:
        return None
    return stripped


def _has_balanced_outer_parens(text: str) -> bool:
    """True when the `(` at index 0 is the one closed by the `)` at the final
    index -- i.e. nesting depth first returns to 0 exactly at the last character.

    A mere `startswith("(") and endswith(")")` check is NOT a balance check: the
    leading and trailing parens can belong to unrelated groups, e.g.
    `(a), to_date(ts)`.
    """
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index == len(text) - 1
    return False


def _normalize_cluster_by(value: Optional[str]) -> Optional[str]:
    """`SHOW` returns clustering keys parenthesized -- `(id, name)` -- while the
    model config yields a bare `id, name`. Strip ONE balanced outer paren pair so
    the two compare equal, and collapse whitespace after commas.

    Deliberately NOT a strip-to-first-paren: that would corrupt an expression
    like `to_date(ts)`, which legitimately contains parens.

    Snowflake may also prefix that parenthesized list with `LINEAR` --
    `LINEAR(ID, VAL)` -- on readback. This is UNVERIFIED against a live
    warehouse: nobody has captured a real `SHOW INTERACTIVE TABLES` value, and
    the closest evidence is a comment on the dynamic-table functional test at
    `tests/functional/relation_tests/dynamic_table_tests/test_configuration_changes.py:406`,
    which notes Snowflake "typically" returns cluster_by with a `LINEAR`
    prefix and deliberately asserts only substring membership rather than
    pinning the format. Because a `cluster_by` diff forces a full refresh,
    guessing wrong here is expensive, so this function tolerates BOTH
    spellings rather than picking one.

    A leading, case-insensitive `LINEAR` is stripped ONLY when the remainder
    (after skipping whitespace) is itself a balanced parenthesized group
    closing at the end of the string -- reusing `_has_balanced_outer_parens`
    on that remainder. This leaves a column or expression literally named
    `linear` alone: bare `linear` has no following paren group to satisfy that
    check, and `linear(a), b` is a multi-key list whose leading group closes
    before the final character, not at it. The single-key case `LINEAR(ts)`
    is genuinely ambiguous -- it could be Snowflake's wrapper around key `ts`,
    or a call to a function named `linear` -- and is deliberately treated as
    the wrapper, consistent with how a bare `(ts)` is already unwrapped.
    """
    if value is None:
        return None
    text = value.strip()
    if text.casefold() in _ABSENT:
        return None
    if text.casefold().startswith("linear"):
        remainder = text[len("linear") :].lstrip()
        if _has_balanced_outer_parens(remainder):
            text = remainder
    if _has_balanced_outer_parens(text):
        text = text[1:-1].strip()
    parts = [part.strip() for part in text.split(",")]
    return ", ".join(part for part in parts if part).casefold()


def _normalize_target_lag(value: Optional[str]) -> Optional[str]:
    """Snowflake canonicalizes lag units on readback (`60 seconds` -> `1 minute`).
    Convert both sides to a comparable count of seconds where possible; fall back
    to a casefolded string so unrecognized forms still compare sanely. The
    fallback also collapses internal whitespace runs to a single space, so
    e.g. `2 weeks` and `2  weeks` still compare equal.

    `DOWNSTREAM` is a legal value and is not a duration.
    """
    if value is None:
        return None
    text = value.strip().casefold()
    if text in _ABSENT:
        return None
    if text == "downstream":
        return "downstream"

    units = {
        "second": 1,
        "seconds": 1,
        "minute": 60,
        "minutes": 60,
        "hour": 3600,
        "hours": 3600,
        "day": 86400,
        "days": 86400,
    }
    parts = text.split()
    if len(parts) == 2 and parts[1] in units:
        try:
            return str(int(parts[0]) * units[parts[1]])
        except ValueError:
            return " ".join(parts)
    return " ".join(parts)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableConfig(SnowflakeRelationConfigBase):
    """Configuration for a Snowflake interactive table.

    `SHOW INTERACTIVE TABLES` exposes only `cluster_by`, `target_lag`,
    `refresh_warehouse`, and `initialization_warehouse` -- note `refresh_warehouse`,
    NOT the `warehouse` column that `SHOW DYNAMIC TABLES` returns. The rewritten
    `text` column is unusable for diffing and is deliberately not read.

    An interactive table is "dynamic" when `target_lag` is set and "static" when it
    is not; a static one reads back NULL for the lag and warehouse columns.

    Every optional field defaults to None on purpose: `from_dict` filters out None
    values, so any other default would silently mask a NULL readback.
    """

    name: Optional[str] = None
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    query: Optional[str] = None
    cluster_by: Optional[str] = None
    target_lag: Optional[str] = None
    snowflake_warehouse: Optional[str] = None
    refresh_warehouse: Optional[str] = None
    snowflake_initialization_warehouse: Optional[str] = None

    # --- normalized views, for COMPARISON ONLY -------------------------------
    # These never replace the stored values: DDL needs the user's exact text.

    @property
    def cluster_by_normalized(self) -> Optional[str]:
        return _normalize_cluster_by(self.cluster_by)

    @property
    def target_lag_normalized(self) -> Optional[str]:
        return _normalize_target_lag(self.target_lag)

    @property
    def refresh_warehouse_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.refresh_warehouse)

    @property
    def warehouse_parameter(self) -> Optional[str]:
        """The value that would be the interactive table's refresh warehouse,
        if this config is dynamic.

        This property is unconditional -- it always resolves a preferred
        warehouse when either field is set, regardless of `target_lag` -- since
        that's just "which of the two warehouse fields wins". When
        `refresh_warehouse` is set it takes precedence, as it is the explicit
        override for the table's self-refresh warehouse; otherwise
        `snowflake_warehouse` serves both roles, the way it does for dynamic
        tables.

        Snowflake requires WAREHOUSE whenever TARGET_LAG is set, and REJECTS it
        otherwise, so this value is only meaningful for the interactive table's
        actual refresh warehouse when the config is dynamic (`is_dynamic`). A
        static table has no refresh warehouse regardless of what this property
        returns; callers comparing against a readback's `refresh_warehouse` must
        gate on `is_dynamic` (or `target_lag_normalized`) themselves.
        """
        return self.refresh_warehouse or self.snowflake_warehouse

    @property
    def warehouse_parameter_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.warehouse_parameter)

    @property
    def snowflake_initialization_warehouse_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.snowflake_initialization_warehouse)

    @property
    def is_dynamic(self) -> bool:
        """A target_lag makes an interactive table auto-refreshing.

        Must use `target_lag_normalized`, not `target_lag`: the literal string
        `'none'` (and other absence spellings) means "no lag" and must read as
        static here too, matching how the changeset builder classifies
        transitions -- see `test_builder_classifies_literal_none_string_target_lag_as_drop`.
        """
        return self.target_lag_normalized is not None

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> dict:
        extra = relation_config.config.extra if relation_config.config else {}

        cluster_by = parse_model.cluster_by(relation_config)
        if not cluster_by or not str(cluster_by).strip():
            raise CompilationError(
                f"Interactive tables require a non-empty `cluster_by` config: "
                f"{relation_config.identifier}"
            )

        if str(extra.get("table_format", "")).strip().casefold() == "iceberg":
            raise CompilationError(
                f"Interactive tables do not support `table_format: iceberg`: "
                f"{relation_config.identifier}"
            )

        if extra.get("transient"):
            raise CompilationError(
                f"Interactive tables do not support `transient: true`: "
                f"{relation_config.identifier}"
            )

        target_lag = extra.get("target_lag")
        warehouse = extra.get("refresh_warehouse") or extra.get("snowflake_warehouse")
        if target_lag and str(target_lag).strip().casefold() not in _ABSENT and not warehouse:
            raise CompilationError(
                f"Interactive tables with `target_lag` set require a warehouse "
                f"(`refresh_warehouse` or `snowflake_warehouse`): {relation_config.identifier}"
            )

        return {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "cluster_by": cluster_by,
            "target_lag": target_lag,
            "snowflake_warehouse": extra.get("snowflake_warehouse"),
            "refresh_warehouse": extra.get("refresh_warehouse"),
            "snowflake_initialization_warehouse": extra.get("snowflake_initialization_warehouse"),
        }

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        row = cls._get_first_row(relation_results["interactive_table"])

        def get(column: str) -> Optional[str]:
            # VERIFIED: agate's MappedSequence.get returns None for a missing
            # column, and _get_first_row yields a keyless Row for empty results
            # whose .get also returns None -- absent column, NULL, and
            # no-such-relation all degrade identically.
            value = row.get(column)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        return {
            "name": get("name"),
            "schema_name": get("schema_name"),
            "database_name": get("database_name"),
            "cluster_by": get("cluster_by"),
            "target_lag": get("target_lag"),
            "refresh_warehouse": get("refresh_warehouse"),
            # Collapsed at LOAD time, not comparison time: '' / 'NONE' are the
            # readback spelling of absence, and the alter macro's `unset` branch
            # keys on this being falsy. Casefolding is a comparison concern
            # (see `_normalize_warehouse` / `*_normalized`), so it stays out of
            # this load-time step -- the stored value stays raw, like the
            # other two warehouse fields.
            "snowflake_initialization_warehouse": _absent_to_none(get("initialization_warehouse")),
        }


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        # Only a value-to-value change is alterable via `ALTER INTERACTIVE TABLE
        # ... SET TARGET_LAG`. Both transitions must rebuild: unsetting a lag
        # (dynamic -> static) is rejected with "invalid value 'null' for
        # property 'TARGET_LAG'" (001422); setting one on an already-static
        # table (static -> dynamic) is rejected with "invalid property
        # 'TARGET_LAG' for 'TABLE'" (001420). Both confirmed live against
        # ktb38830, 2026-08-25.
        return self.action != RelationConfigChangeAction.alter


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableClusterByConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        # DIVERGES from dynamic tables (which return False and emit
        # `ALTER DYNAMIC TABLE ... CLUSTER BY`). Snowflake rejects
        # `ALTER ... CLUSTER BY` on an interactive table with 001003.
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableRefreshWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableInitializationWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass
class SnowflakeInteractiveTableConfigChangeset:
    target_lag: Optional[SnowflakeInteractiveTableTargetLagConfigChange] = None
    cluster_by: Optional[SnowflakeInteractiveTableClusterByConfigChange] = None
    refresh_warehouse: Optional[SnowflakeInteractiveTableRefreshWarehouseConfigChange] = None
    snowflake_initialization_warehouse: Optional[
        SnowflakeInteractiveTableInitializationWarehouseConfigChange
    ] = None

    @property
    def _changes(self) -> list:
        return [
            self.target_lag,
            self.cluster_by,
            self.refresh_warehouse,
            self.snowflake_initialization_warehouse,
        ]

    @property
    def requires_full_refresh(self) -> bool:
        return any(change.requires_full_refresh for change in self._changes if change)

    @property
    def has_changes(self) -> bool:
        return any(change is not None for change in self._changes)
