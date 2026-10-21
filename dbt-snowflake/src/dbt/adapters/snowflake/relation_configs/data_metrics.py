import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, cast

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError
from typing_extensions import Self

from dbt.adapters.snowflake import parse_model
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

if TYPE_CHECKING:
    import agate


FUNCTION_NAME_COLUMNS = ("metric_database_name", "metric_schema_name", "metric_name")

_FUNCTION_ENTRY_KEYS = {"function", "columns"}

_MINUTE_SCHEDULE = re.compile(r"(\d+)\s+MINUTES?", re.IGNORECASE)
# One name part: a double-quoted identifier (dots allowed inside, `""` escapes a
# quote) or anything without a dot or quote. Characters are not validated.
_NAME_PART = r'\s*(?:"(?:[^"]|"")*"|[^."]+)\s*'
_FUNCTION_NAME = re.compile(rf"({_NAME_PART})\.({_NAME_PART})\.({_NAME_PART})")


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeIdentifier:
    """One identifier part. `sql` is what gets rendered into DDL; `normalized` is
    what Snowflake stores (unquoted parts fold to upper case, quoted parts are
    kept exactly), so it compares against metadata readback."""

    sql: str
    normalized: str

    @classmethod
    def from_metadata(cls, name: str) -> Self:
        # Metadata reports the stored name, so always quote it on the way back out.
        return cls(sql='"' + name.replace('"', '""') + '"', normalized=name)


def _parse_identifier(text: Any, setting: str) -> SnowflakeIdentifier:
    """Normalizes a name the way Snowflake stores it, so it compares against metadata.
    Quote structure is checked; other name validity is left to Snowflake."""
    if not isinstance(text, str):
        raise DbtConfigError(f"Expected an identifier string in {setting}, got {text!r}")
    stripped = text.strip()
    if len(stripped) >= 2 and stripped.startswith('"') and stripped.endswith('"'):
        inner = stripped[1:-1]
        # every quote inside must be part of a doubled escape
        if '"' in inner.replace('""', ""):
            raise DbtConfigError(
                f"Invalid quoted identifier {text!r} in {setting}: "
                'double quotes inside an identifier must be escaped as ""'
            )
        return SnowflakeIdentifier(sql=stripped, normalized=inner.replace('""', '"'))
    if '"' in stripped:
        raise DbtConfigError(
            f"Invalid identifier {text!r} in {setting}: "
            "double quotes must enclose the entire identifier"
        )
    return SnowflakeIdentifier(sql=stripped, normalized=stripped.upper())


def _parse_function_name(text: Any, setting: str) -> Tuple[SnowflakeIdentifier, ...]:
    if not isinstance(text, str):
        raise DbtConfigError(f"Expected a function name string in {setting}, got {text!r}")
    match = _FUNCTION_NAME.fullmatch(text)
    # Metadata always reports database.schema.name, so a partially qualified
    # name could never be matched against it reliably.
    if match is None:
        raise DbtConfigError(
            f"Invalid data metric function {text!r} in {setting}: must be fully qualified "
            "as <database>.<schema>.<function>, e.g. "
            "SNOWFLAKE.CORE.NULL_COUNT"
        )
    return tuple(_parse_identifier(part, setting) for part in match.groups())


def _parse_ref_arguments(raw: Any) -> List[str]:
    """`REF_ARGUMENTS` is a JSON array like `[{"domain":"COLUMN","id":1,"name":"STATUS"}]`."""
    value = raw
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise DbtRuntimeError(
                f"Could not parse data metric function REF_ARGUMENTS {raw!r}: {exc}"
            ) from exc
    if not isinstance(value, list):
        raise DbtRuntimeError(
            f"Could not parse data metric function REF_ARGUMENTS {raw!r}: expected an array"
        )

    names = []
    for argument in value:
        if not isinstance(argument, dict):
            raise DbtRuntimeError(
                f"Could not parse data metric function REF_ARGUMENTS {raw!r}: "
                f"unexpected element {argument!r}"
            )
        domain = argument.get("domain")
        name = argument.get("name")
        if not isinstance(domain, str) or domain.upper() != "COLUMN":
            # e.g. a table argument of a multi-table DMF; dbt cannot render it back.
            raise DbtRuntimeError(
                f"Unsupported data metric function argument {argument!r}: only column "
                "arguments can be managed by dbt"
            )
        if not isinstance(name, str) or not name:
            raise DbtRuntimeError(
                f"Could not parse data metric function REF_ARGUMENTS {raw!r}: "
                f"missing column name in {argument!r}"
            )
        names.append(name)
    return names


def _lowercase_columns(table: "agate.Table") -> "agate.Table":
    return table.rename(column_names=[name.lower() for name in table.column_names])


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDataMetricFunctionAssociation:
    """One `ADD DATA METRIC FUNCTION <function> ON (<columns>)` association.
    An empty `columns` tuple is a zero-argument DMF such as ROW_COUNT ON ()."""

    function: Tuple[SnowflakeIdentifier, ...]
    columns: Tuple[SnowflakeIdentifier, ...] = field(default_factory=tuple)

    @property
    def key(self) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        # Column order is significant: the arguments bind positionally.
        return (
            tuple(part.normalized for part in self.function),
            tuple(column.normalized for column in self.columns),
        )

    @property
    def function_sql(self) -> str:
        return ".".join(part.sql for part in self.function)

    @property
    def columns_sql(self) -> str:
        return ", ".join(column.sql for column in self.columns)

    @classmethod
    def from_config_entry(cls, entry: Any, setting: str) -> Self:
        if not isinstance(entry, dict):
            raise DbtConfigError(
                f"Unexpected {setting}: expected a mapping with `function` and optional "
                f"`columns`, got {entry!r}"
            )
        unknown = set(entry) - _FUNCTION_ENTRY_KEYS
        if unknown:
            raise DbtConfigError(
                f"Unsupported key(s) {sorted(unknown)} in {setting}; only `function` and "
                "`columns` are supported (expectations are not supported yet)"
            )
        if "function" not in entry:
            raise DbtConfigError(f"{setting} is missing the required `function` key")

        columns = entry.get("columns")
        if columns is None:
            columns = []
        if not isinstance(columns, (list, tuple)):
            raise DbtConfigError(
                f"Unexpected `columns` in {setting}: expected a list of column names, "
                f"got {columns!r}"
            )
        function = entry["function"]
        return cls(
            function=_parse_function_name(function, setting),
            columns=tuple(_parse_identifier(column, setting) for column in columns),
        )

    @classmethod
    def from_metadata_row(cls, row: "agate.Row") -> Self:
        parts = [row.get(column) for column in FUNCTION_NAME_COLUMNS]
        if not all(isinstance(part, str) and part for part in parts):
            raise DbtRuntimeError(
                f"Could not parse data metric function reference: unexpected metric name {parts!r}"
            )
        return cls(
            function=tuple(SnowflakeIdentifier.from_metadata(part) for part in parts),
            columns=tuple(
                SnowflakeIdentifier.from_metadata(name)
                for name in _parse_ref_arguments(row.get("ref_arguments"))
            ),
        )


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDataMetricsConfig(SnowflakeRelationConfigBase):
    """
    DMF state of a table or view.

    `None` means "not managed": absent config leaves whatever is on the relation
    alone, while an empty `functions` tuple means "no associations" and removes
    every existing one. On the metadata side, `None` means the value was not read.
    """

    schedule: Optional[str] = None
    functions: Optional[Tuple[SnowflakeDataMetricFunctionAssociation, ...]] = None

    @property
    def schedule_normalized(self) -> Optional[str]:
        if self.schedule is None:
            return None
        schedule = " ".join(self.schedule.split()).upper()
        if match := _MINUTE_SCHEDULE.fullmatch(schedule):
            return f"{int(match.group(1))} MINUTE"
        return schedule

    @property
    def is_configured(self) -> bool:
        return self.schedule is not None or self.functions is not None

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        if (
            relation_config.config is not None
            and "data_metric_schedule" in relation_config.config
            and relation_config.config.get("data_metric_schedule") is None
        ):
            raise DbtConfigError(
                "Invalid data_metric_schedule: null is not allowed; omit the key to leave "
                "the schedule unmanaged"
            )

        schedule = parse_model.data_metric_schedule(relation_config)
        if schedule is not None and not isinstance(schedule, str):
            raise DbtConfigError(
                f"Unexpected data_metric_schedule configuration: expected a string, got {schedule!r}"
            )

        entries = parse_model.data_metric_functions(relation_config)
        if entries is not None and not isinstance(entries, (list, tuple)):
            raise DbtConfigError(
                "Unexpected data_metric_functions configuration: expected a list of "
                f"{{function, columns}} mappings, got {entries!r}"
            )
        # Model config owns the full set of DMF associations.
        functions: Optional[Tuple[SnowflakeDataMetricFunctionAssociation, ...]] = None
        if entries is not None:
            functions_by_key: Dict[
                Tuple[Tuple[str, ...], Tuple[str, ...]],
                SnowflakeDataMetricFunctionAssociation,
            ] = {}
            for i, entry in enumerate(entries):
                association = SnowflakeDataMetricFunctionAssociation.from_config_entry(
                    entry, f"data_metric_functions[{i}]"
                )
                functions_by_key.setdefault(association.key, association)
            functions = tuple(functions_by_key.values())

        return {
            "schedule": schedule,
            "functions": functions,
        }

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        """
        Expects the dict from `SnowflakeAdapter.describe_data_metrics`:
        - "function_references": DATA_METRIC_FUNCTION_REFERENCES rows
        - "schedule_parameters": `SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE'` rows
        Either key may be missing when that half of the config is not managed.
        """
        references = relation_results.get("function_references")
        functions = None
        if references is not None:
            references = _lowercase_columns(cast("agate.Table", references))
            functions_by_key: Dict[Any, SnowflakeDataMetricFunctionAssociation] = {}
            for row in references.rows:
                association = SnowflakeDataMetricFunctionAssociation.from_metadata_row(row)
                functions_by_key.setdefault(association.key, association)
            functions = tuple(functions_by_key.values())

        parameters = relation_results.get("schedule_parameters")
        schedule = None
        if parameters is not None:
            parameters = _lowercase_columns(cast("agate.Table", parameters))
            parameter = parameters.rows[0]
            value = parameter.get("value")
            if value is not None:
                if not isinstance(value, str):
                    raise DbtRuntimeError(
                        "Could not parse DATA_METRIC_SCHEDULE value from "
                        f"SHOW PARAMETERS: {value!r}"
                    )
                schedule = value

        return {"schedule": schedule, "functions": functions}


@dataclass
class SnowflakeDataMetricsConfigChangeset:
    """Applied in order: schedule, adds, drops. Snowflake accepts ADD before a
    schedule is set, so the schedule goes first only for readability. Adds precede
    drops so a failed add does not first remove an existing association."""

    schedule: Optional[str] = None
    adds: List[SnowflakeDataMetricFunctionAssociation] = field(default_factory=list)
    drops: List[SnowflakeDataMetricFunctionAssociation] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return self.schedule is not None or bool(self.adds) or bool(self.drops)
