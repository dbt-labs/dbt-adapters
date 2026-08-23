from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class CreateFromQueryStrategy(str, Enum):
    """Closed set of physical strategies for creating a relation from a query."""

    CTAS = "ctas"
    CREATE_THEN_INSERT = "create_then_insert"
    UNSUPPORTED = "unsupported"


class DdlAtomicity(str, Enum):
    """Atomicity promised by one physical DDL operation."""

    UNKNOWN = "unknown"
    STATEMENT = "statement"
    TRANSACTION = "transaction"
    BEST_EFFORT = "best_effort"
    NONE = "none"


@dataclass(frozen=True)
class PlanProvenance:
    """Rule and evidence that caused a resolver to select a plan."""

    rule: str
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.rule, str) or not isinstance(self.detail, str):
            raise TypeError("Plan provenance rule and detail must be strings")
        if not self.rule.strip():
            raise ValueError("Plan provenance rule must not be empty")
        if not self.detail.strip():
            raise ValueError("Plan provenance detail must not be empty")

    def to_dict(self) -> Dict[str, str]:
        return {"rule": self.rule, "detail": self.detail}


@dataclass(frozen=True)
class CreateFromQueryPlan:
    """Validated physical plan consumed by a create-from-query renderer.

    Relation, catalog, format, provider, and runtime facts belong in the
    resolver that constructs this value. Renderers receive only this resolved
    decision plus the relation and query they must render.
    """

    strategy: CreateFromQueryStrategy
    atomicity: DdlAtomicity
    temporary: bool
    provenance: Tuple[PlanProvenance, ...]
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, CreateFromQueryStrategy):
            raise TypeError("Create-from-query plan strategy must be a CreateFromQueryStrategy")
        if not isinstance(self.atomicity, DdlAtomicity):
            raise TypeError("Create-from-query plan atomicity must be a DdlAtomicity")
        if not isinstance(self.temporary, bool):
            raise TypeError("Create-from-query plan 'temporary' must be a bool")
        if not isinstance(self.provenance, tuple):
            raise TypeError("Create-from-query plan provenance must be an immutable tuple")
        if not self.provenance:
            raise ValueError("Create-from-query plan must include provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Create-from-query plan provenance must contain PlanProvenance")
        if self.reason is not None and not isinstance(self.reason, str):
            raise TypeError("Create-from-query plan reason must be a string")

        if self.strategy == CreateFromQueryStrategy.UNSUPPORTED:
            if not self.reason or not self.reason.strip():
                raise ValueError("Unsupported create-from-query plan must include a reason")
            if self.atomicity != DdlAtomicity.NONE:
                raise ValueError("Unsupported create-from-query plan cannot promise atomicity")
        elif self.reason is not None:
            raise ValueError(
                "Supported create-from-query plan cannot include an unsupported reason"
            )

    @classmethod
    def ctas(
        cls,
        *,
        temporary: bool,
        atomicity: DdlAtomicity,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryPlan":
        return cls(
            strategy=CreateFromQueryStrategy.CTAS,
            atomicity=atomicity,
            temporary=temporary,
            provenance=provenance,
        )

    @classmethod
    def create_then_insert(
        cls,
        *,
        temporary: bool,
        atomicity: DdlAtomicity,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryPlan":
        return cls(
            strategy=CreateFromQueryStrategy.CREATE_THEN_INSERT,
            atomicity=atomicity,
            temporary=temporary,
            provenance=provenance,
        )

    @classmethod
    def unsupported(
        cls,
        *,
        temporary: bool,
        reason: str,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryPlan":
        return cls(
            strategy=CreateFromQueryStrategy.UNSUPPORTED,
            atomicity=DdlAtomicity.NONE,
            temporary=temporary,
            provenance=provenance,
            reason=reason,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return stable primitive data suitable for fixtures and cross-runtime exchange."""

        return {
            "strategy": self.strategy.value,
            "atomicity": self.atomicity.value,
            "temporary": self.temporary,
            "provenance": [item.to_dict() for item in self.provenance],
            "reason": self.reason,
        }
