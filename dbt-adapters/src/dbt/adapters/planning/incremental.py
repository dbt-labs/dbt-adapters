from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from dbt.adapters.planning.create_from_query import DdlAtomicity, PlanProvenance


class IncrementalMutationStrategy(str, Enum):
    """Closed physical strategies understood by the built-in incremental renderer."""

    ADAPTER_DEFAULT = "adapter_default"
    APPEND = "append"
    DELETE_INSERT = "delete_insert"
    MERGE = "merge"
    INSERT_OVERWRITE = "insert_overwrite"
    MICROBATCH = "microbatch"
    CUSTOM = "custom"
    UNSUPPORTED = "unsupported"


class IncrementalSchemaChangeStrategy(str, Enum):
    """Closed schema reconciliation policies understood by the materialization."""

    IGNORE = "ignore"
    APPEND_NEW_COLUMNS = "append_new_columns"
    SYNC_ALL_COLUMNS = "sync_all_columns"
    FAIL = "fail"


@dataclass(frozen=True)
class IncrementalSchemaChangePlan:
    """Validated schema reconciliation intent, resolved before macro execution."""

    requested_strategy: str
    strategy: IncrementalSchemaChangeStrategy
    provenance: Tuple[PlanProvenance, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.requested_strategy, str) or not self.requested_strategy.strip():
            raise ValueError("Schema change plan requested strategy must be a non-empty string")
        if not isinstance(self.strategy, IncrementalSchemaChangeStrategy):
            raise TypeError(
                "Schema change plan strategy must be an IncrementalSchemaChangeStrategy"
            )
        if not isinstance(self.provenance, tuple):
            raise TypeError("Schema change plan provenance must be an immutable tuple")
        if not self.provenance:
            raise ValueError("Schema change plan must include provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Schema change plan provenance must contain PlanProvenance")

    @property
    def was_coerced(self) -> bool:
        return self.requested_strategy != self.strategy.value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requested_strategy": self.requested_strategy,
            "strategy": self.strategy.value,
            "provenance": [item.to_dict() for item in self.provenance],
        }


UniqueKey = Optional[Union[str, Tuple[str, ...]]]


@dataclass(frozen=True)
class IncrementalMutationArguments:
    """Typed late-bound inputs for an incremental strategy renderer."""

    target_relation: Any
    temp_relation: Any
    unique_key: UniqueKey
    dest_columns: Tuple[Any, ...]
    incremental_predicates: Optional[Tuple[str, ...]]

    def __post_init__(self) -> None:
        if self.target_relation is None:
            raise ValueError("Incremental arguments require a target relation")
        if self.temp_relation is None:
            raise ValueError("Incremental arguments require a temporary relation")
        if not isinstance(self.dest_columns, tuple):
            raise TypeError("Incremental destination columns must be an immutable tuple")
        if isinstance(self.unique_key, str):
            if not self.unique_key.strip():
                raise ValueError("Incremental unique key cannot be empty")
        elif self.unique_key is not None:
            if not isinstance(self.unique_key, tuple):
                raise TypeError("Incremental unique key columns must be an immutable tuple")
            if not self.unique_key or not all(
                isinstance(key, str) and key.strip() for key in self.unique_key
            ):
                raise ValueError("Incremental unique key columns must be non-empty strings")
        if self.incremental_predicates is not None:
            if not isinstance(self.incremental_predicates, tuple):
                raise TypeError("Incremental predicates must be an immutable tuple")
            if not all(
                isinstance(predicate, str) and predicate.strip()
                for predicate in self.incremental_predicates
            ):
                raise ValueError("Incremental predicates must be non-empty strings")

    @classmethod
    def from_values(
        cls,
        *,
        target_relation: Any,
        temp_relation: Any,
        unique_key: Optional[Union[str, Sequence[str]]],
        dest_columns: Iterable[Any],
        incremental_predicates: Optional[Sequence[str]],
    ) -> "IncrementalMutationArguments":
        normalized_unique_key: UniqueKey
        if unique_key is None or isinstance(unique_key, str):
            normalized_unique_key = unique_key
        else:
            normalized_unique_key = tuple(unique_key)

        if isinstance(incremental_predicates, str):
            raise TypeError("Incremental predicates must be a sequence, not a string")
        normalized_predicates = None
        if incremental_predicates is not None:
            normalized_predicates = tuple(incremental_predicates)
        return cls(
            target_relation=target_relation,
            temp_relation=temp_relation,
            unique_key=normalized_unique_key,
            dest_columns=tuple(dest_columns),
            incremental_predicates=normalized_predicates,
        )

    def to_macro_dict(self) -> Dict[str, Any]:
        unique_key: Optional[Union[str, List[str]]]
        if isinstance(self.unique_key, tuple):
            unique_key = list(self.unique_key)
        else:
            unique_key = self.unique_key

        return {
            "target_relation": self.target_relation,
            "temp_relation": self.temp_relation,
            "unique_key": unique_key,
            "dest_columns": list(self.dest_columns),
            "incremental_predicates": (
                None if self.incremental_predicates is None else list(self.incremental_predicates)
            ),
        }


@dataclass(frozen=True)
class IncrementalMutationPlan:
    """Validated selection of one incremental mutation renderer."""

    requested_strategy: str
    strategy: IncrementalMutationStrategy
    renderer_macro: Optional[str]
    atomicity: DdlAtomicity
    provenance: Tuple[PlanProvenance, ...]
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.requested_strategy, str) or not self.requested_strategy.strip():
            raise ValueError("Incremental plan requested strategy must be a non-empty string")
        if not isinstance(self.strategy, IncrementalMutationStrategy):
            raise TypeError("Incremental plan strategy must be an IncrementalMutationStrategy")
        if self.renderer_macro is not None and not isinstance(self.renderer_macro, str):
            raise TypeError("Incremental plan renderer macro must be a string")
        if not isinstance(self.atomicity, DdlAtomicity):
            raise TypeError("Incremental plan atomicity must be a DdlAtomicity")
        if not isinstance(self.provenance, tuple):
            raise TypeError("Incremental plan provenance must be an immutable tuple")
        if not self.provenance:
            raise ValueError("Incremental plan must include provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Incremental plan provenance must contain PlanProvenance")
        if self.reason is not None and not isinstance(self.reason, str):
            raise TypeError("Incremental plan reason must be a string")

        if self.strategy == IncrementalMutationStrategy.UNSUPPORTED:
            if self.renderer_macro is not None:
                raise ValueError("Unsupported incremental plan cannot select a renderer macro")
            if self.atomicity != DdlAtomicity.NONE:
                raise ValueError("Unsupported incremental plan cannot promise atomicity")
            if not self.reason or not self.reason.strip():
                raise ValueError("Unsupported incremental plan must include a reason")
        else:
            if not self.renderer_macro or not self.renderer_macro.strip():
                raise ValueError("Supported incremental plan must select a renderer macro")
            if self.reason is not None:
                raise ValueError("Supported incremental plan cannot include an unsupported reason")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requested_strategy": self.requested_strategy,
            "strategy": self.strategy.value,
            "renderer_macro": self.renderer_macro,
            "atomicity": self.atomicity.value,
            "provenance": [item.to_dict() for item in self.provenance],
            "reason": self.reason,
        }


_BUILTIN_STRATEGIES = {
    "default": IncrementalMutationStrategy.ADAPTER_DEFAULT,
    "append": IncrementalMutationStrategy.APPEND,
    "delete+insert": IncrementalMutationStrategy.DELETE_INSERT,
    "merge": IncrementalMutationStrategy.MERGE,
    "insert_overwrite": IncrementalMutationStrategy.INSERT_OVERWRITE,
    "microbatch": IncrementalMutationStrategy.MICROBATCH,
}

_SCHEMA_CHANGE_STRATEGIES = {
    strategy.value: strategy for strategy in IncrementalSchemaChangeStrategy
}


def resolve_incremental_schema_change_plan(
    requested_strategy: Optional[str],
    *,
    default: str = IncrementalSchemaChangeStrategy.IGNORE.value,
) -> IncrementalSchemaChangePlan:
    """Resolve schema reconciliation config into a closed, validated policy."""

    if default not in _SCHEMA_CHANGE_STRATEGIES:
        raise ValueError(f"Unknown default incremental schema change strategy '{default}'")

    requested = requested_strategy or default
    strategy = _SCHEMA_CHANGE_STRATEGIES.get(requested)
    if strategy is not None:
        return IncrementalSchemaChangePlan(
            requested_strategy=requested,
            strategy=strategy,
            provenance=(
                PlanProvenance(
                    rule=f"incremental.schema_change.{strategy.value}",
                    detail=f"Requested schema change strategy '{requested}' is valid",
                ),
            ),
        )

    resolved = _SCHEMA_CHANGE_STRATEGIES[default]
    return IncrementalSchemaChangePlan(
        requested_strategy=requested,
        strategy=resolved,
        provenance=(
            PlanProvenance(
                rule="incremental.schema_change.invalid_default",
                detail=(
                    f"Invalid value for on_schema_change ({requested}) specified. "
                    f"Setting default value of {default}."
                ),
            ),
        ),
    )


def resolve_incremental_mutation_plan(
    requested_strategy: Optional[str],
    *,
    valid_strategies: Iterable[str],
    builtin_strategies: Iterable[str],
) -> IncrementalMutationPlan:
    """Resolve user intent and adapter support into one renderer selection."""

    requested = requested_strategy or "default"
    valid = frozenset(valid_strategies) | {"default"}
    builtin = frozenset(builtin_strategies)

    if requested in builtin and requested not in valid:
        reason = f"The incremental strategy '{requested}' is not valid for this adapter"
        return IncrementalMutationPlan(
            requested_strategy=requested,
            strategy=IncrementalMutationStrategy.UNSUPPORTED,
            renderer_macro=None,
            atomicity=DdlAtomicity.NONE,
            provenance=(
                PlanProvenance(
                    rule="incremental.requested_strategy.unsupported",
                    detail=reason,
                ),
            ),
            reason=reason,
        )

    strategy = _BUILTIN_STRATEGIES.get(requested, IncrementalMutationStrategy.CUSTOM)
    renderer_macro = f"get_incremental_{requested.replace('+', '_')}_sql"
    return IncrementalMutationPlan(
        requested_strategy=requested,
        strategy=strategy,
        renderer_macro=renderer_macro,
        atomicity=DdlAtomicity.UNKNOWN,
        provenance=(
            PlanProvenance(
                rule=f"incremental.requested_strategy.{strategy.value}",
                detail=f"Requested strategy '{requested}' resolved to '{renderer_macro}'",
            ),
        ),
    )
