import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults
from dbt_common.exceptions import CompilationError
from typing_extensions import Self

from dbt.adapters.snowflake.parse_model import (
    data_metric_functions as _configured_data_metric_functions,
    data_metric_schedule as _configured_data_metric_schedule,
)
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

if TYPE_CHECKING:
    import agate


def _normalize_identifier(value: str) -> str:
    """Normalize a single Snowflake identifier for case-insensitive comparison.

    Unquoted identifiers are case-insensitive and stored upper case by Snowflake,
    while quoted identifiers are case-sensitive. Surrounding double quotes are
    stripped, and unquoted identifiers are upper cased, so that a configured
    ``trade_id`` matches the ``TRADE_ID`` that Snowflake reports back.
    """
    value = value.strip()
    if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    return value.upper()


def _normalize_metric_name(name: str) -> str:
    """Normalize a fully qualified data metric function name part by part."""
    return ".".join(_normalize_identifier(part) for part in name.split("."))


def _parse_ref_arguments(raw: Any) -> Tuple[str, ...]:
    """Extract the ordered column names from the ``ref_arguments`` value returned by
    ``INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES``.

    Snowflake documents ``ref_arguments`` as an ``ARRAY``; the connector may surface
    it either as a parsed list or as a JSON string. Each element identifies one
    referenced column, so the ``name`` of each element is collected in order. The
    parser is deliberately tolerant of shape so a schema change on Snowflake's side
    degrades to "no arguments" rather than raising mid-build.
    """
    if raw is None:
        return ()

    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return ()

    if not isinstance(raw, (list, tuple)):
        return ()

    arguments: List[str] = []
    for element in raw:
        if isinstance(element, dict):
            name = element.get("name", element.get("NAME"))
            if name is not None:
                arguments.append(str(name))
        elif element is not None:
            arguments.append(str(element))
    return tuple(arguments)


@dataclass(frozen=True)
class SnowflakeDataMetricFunction:
    """A single data metric function association.

    ``name`` is the fully qualified DMF name (for example ``snowflake.core.null_count``
    or a fully qualified reference to a pre-existing custom DMF) and ``arguments`` is
    the ordered tuple of column expressions the DMF is applied to.

    ``name`` and ``arguments`` preserve the text needed to render DDL. ``comparison_key``
    provides the case-normalized view used to diff a desired association against an
    existing one, so that ``trade_id`` and ``TRADE_ID`` are treated as the same column.
    """

    name: str
    arguments: Tuple[str, ...]

    @property
    def comparison_key(self) -> Tuple[str, Tuple[str, ...]]:
        return (
            _normalize_metric_name(self.name),
            tuple(_normalize_identifier(argument) for argument in self.arguments),
        )

    @property
    def arguments_clause(self) -> str:
        """The comma-separated column list rendered into the ``ON (...)`` clause."""
        return ", ".join(self.arguments)


@dataclass(frozen=True)
class SnowflakeDataMetricFunctionsConfig(SnowflakeRelationConfigBase):
    """The data metric functions and evaluation schedule configured on a relation.

    This mirrors the structure of :class:`SnowflakeDynamicTableConfig`: it can be built
    from a model's config (the desired state) via ``from_relation_config`` or from
    Snowflake introspection (the current state) via ``from_relation_results``, and the
    two are diffed into a :class:`SnowflakeDataMetricFunctionsConfigChangeset`.
    """

    schedule: Optional[str] = None
    metric_functions: Tuple[SnowflakeDataMetricFunction, ...] = field(default_factory=tuple)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        metric_functions = tuple(
            SnowflakeDataMetricFunction(
                name=metric_function["name"],
                arguments=tuple(metric_function["arguments"]),
            )
            for metric_function in config_dict.get("metric_functions", [])
        )
        return cls(schedule=config_dict.get("schedule"), metric_functions=metric_functions)

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        schedule = _configured_data_metric_schedule(relation_config)
        metric_functions = _configured_data_metric_functions(relation_config)

        # Snowflake requires a schedule on the table before a data metric function can
        # be added, so a config that asks for DMFs without one can never converge.
        if metric_functions and not schedule:
            raise CompilationError(
                "Invalid data metric function config: `data_metric_schedule` is required "
                "when `data_metric_functions` are configured. Snowflake requires a schedule "
                "on the relation before a data metric function can be added."
            )

        return {"schedule": schedule, "metric_functions": metric_functions}

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        references: "agate.Table" = relation_results["data_metric_function_references"]

        schedule: Optional[str] = None
        metric_functions: List[Dict[str, Any]] = []
        for row in references.rows:
            # The schedule is a relation-level property, so every row reports the same
            # effective value; the last non-empty one is kept.
            row_schedule = row.get("schedule")
            if row_schedule:
                schedule = str(row_schedule)

            name = ".".join(
                str(part)
                for part in (
                    row.get("metric_database_name"),
                    row.get("metric_schema_name"),
                    row.get("metric_name"),
                )
                if part
            )
            if not name:
                continue

            metric_functions.append(
                {"name": name, "arguments": _parse_ref_arguments(row.get("ref_arguments"))}
            )

        return {"schedule": schedule, "metric_functions": metric_functions}


@dataclass(frozen=True)
class SnowflakeDataMetricFunctionsConfigChangeset:
    """The set of ALTER operations that converge a relation onto its configured DMFs.

    ``schedule`` is the schedule to set (``None`` when it already matches), ``to_add``
    are the associations to create, and ``to_drop`` are the managed associations to
    remove. The apply macro emits ``schedule`` first because Snowflake requires a
    schedule before any ADD.
    """

    schedule: Optional[str] = None
    to_add: Tuple[SnowflakeDataMetricFunction, ...] = field(default_factory=tuple)
    to_drop: Tuple[SnowflakeDataMetricFunction, ...] = field(default_factory=tuple)

    @property
    def has_changes(self) -> bool:
        return self.schedule is not None or bool(self.to_add) or bool(self.to_drop)
