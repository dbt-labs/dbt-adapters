from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults

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
    """
    if value is None:
        return None
    text = value.strip()
    if text.casefold() in _ABSENT:
        return None
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
    def snowflake_warehouse_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.snowflake_warehouse)

    @property
    def snowflake_initialization_warehouse_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.snowflake_initialization_warehouse)

    @property
    def is_dynamic(self) -> bool:
        """A target_lag makes an interactive table auto-refreshing."""
        return self.target_lag is not None

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> dict:
        extra = relation_config.config.extra if relation_config.config else {}
        return {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "cluster_by": parse_model.cluster_by(relation_config),
            "target_lag": extra.get("target_lag"),
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
