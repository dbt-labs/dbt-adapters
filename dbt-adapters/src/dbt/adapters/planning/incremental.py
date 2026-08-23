from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Tuple

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
