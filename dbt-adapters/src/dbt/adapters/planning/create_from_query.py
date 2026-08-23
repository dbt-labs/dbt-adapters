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


class CatalogBindingState(str, Enum):
    """How completely a logical catalog binding has been resolved."""

    UNBOUND = "unbound"
    NAMED = "named"
    RESOLVED = "resolved"


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


def _validate_optional_string(value: Optional[str], field_name: str) -> None:
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string when provided")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string when provided")


@dataclass(frozen=True)
class RelationFacts:
    """Resolved logical and physical address facts for a relation."""

    database: Optional[str]
    schema: Optional[str]
    identifier: Optional[str]
    relation_type: Optional[str]

    def __post_init__(self) -> None:
        _validate_optional_string(self.database, "Relation database")
        _validate_optional_string(self.schema, "Relation schema")
        _validate_optional_string(self.identifier, "Relation identifier")
        _validate_optional_string(self.relation_type, "Relation type")

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "database": self.database,
            "schema": self.schema,
            "identifier": self.identifier,
            "relation_type": self.relation_type,
        }


@dataclass(frozen=True)
class CatalogFacts:
    """Resolved catalog identity and storage binding facts."""

    state: CatalogBindingState
    integration_name: Optional[str] = None
    catalog_type: Optional[str] = None
    catalog_name: Optional[str] = None
    catalog_database: Optional[str] = None
    external_volume: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, CatalogBindingState):
            raise TypeError("Catalog binding state must be a CatalogBindingState")
        for value, field_name in (
            (self.integration_name, "Catalog integration name"),
            (self.catalog_type, "Catalog type"),
            (self.catalog_name, "Platform catalog name"),
            (self.catalog_database, "Catalog database"),
            (self.external_volume, "External volume"),
        ):
            _validate_optional_string(value, field_name)

        if self.state == CatalogBindingState.UNBOUND:
            if any(
                value is not None
                for value in (
                    self.integration_name,
                    self.catalog_type,
                    self.catalog_name,
                    self.catalog_database,
                    self.external_volume,
                )
            ):
                raise ValueError("Unbound catalog facts cannot include catalog values")
        elif self.state == CatalogBindingState.NAMED:
            if self.integration_name is None:
                raise ValueError("Named catalog facts require an integration name")
            if any(
                value is not None
                for value in (
                    self.catalog_type,
                    self.catalog_name,
                    self.catalog_database,
                    self.external_volume,
                )
            ):
                raise ValueError("Named catalog facts cannot include resolved catalog values")
        elif self.catalog_type is None:
            raise ValueError("Resolved catalog facts require a catalog type")

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "state": self.state.value,
            "integration_name": self.integration_name,
            "catalog_type": self.catalog_type,
            "catalog_name": self.catalog_name,
            "catalog_database": self.catalog_database,
            "external_volume": self.external_volume,
        }


@dataclass(frozen=True)
class FormatFacts:
    """Resolved table and file format facts."""

    table_format: Optional[str] = None
    file_format: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_optional_string(self.table_format, "Table format")
        _validate_optional_string(self.file_format, "File format")

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "table_format": self.table_format,
            "file_format": self.file_format,
        }


@dataclass(frozen=True)
class RuntimeFacts:
    """Resolved execution runtime identity available to a strategy resolver."""

    engine: str
    version: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.engine, str):
            raise TypeError("Runtime engine must be a string")
        if not self.engine.strip():
            raise ValueError("Runtime engine must be a non-empty string")
        _validate_optional_string(self.version, "Runtime version")

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {"engine": self.engine, "version": self.version}


@dataclass(frozen=True)
class CreateFromQueryFacts:
    """Typed resolver inputs assembled before strategy selection."""

    relation: RelationFacts
    catalog: CatalogFacts
    format: FormatFacts
    runtime: RuntimeFacts

    def __post_init__(self) -> None:
        if not isinstance(self.relation, RelationFacts):
            raise TypeError("Create-from-query relation facts must be RelationFacts")
        if not isinstance(self.catalog, CatalogFacts):
            raise TypeError("Create-from-query catalog facts must be CatalogFacts")
        if not isinstance(self.format, FormatFacts):
            raise TypeError("Create-from-query format facts must be FormatFacts")
        if not isinstance(self.runtime, RuntimeFacts):
            raise TypeError("Create-from-query runtime facts must be RuntimeFacts")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "relation": self.relation.to_dict(),
            "catalog": self.catalog.to_dict(),
            "format": self.format.to_dict(),
            "runtime": self.runtime.to_dict(),
        }


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
    facts: CreateFromQueryFacts
    provenance: Tuple[PlanProvenance, ...]
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, CreateFromQueryStrategy):
            raise TypeError("Create-from-query plan strategy must be a CreateFromQueryStrategy")
        if not isinstance(self.atomicity, DdlAtomicity):
            raise TypeError("Create-from-query plan atomicity must be a DdlAtomicity")
        if not isinstance(self.temporary, bool):
            raise TypeError("Create-from-query plan 'temporary' must be a bool")
        if not isinstance(self.facts, CreateFromQueryFacts):
            raise TypeError("Create-from-query plan facts must be CreateFromQueryFacts")
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
        facts: CreateFromQueryFacts,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryPlan":
        return cls(
            strategy=CreateFromQueryStrategy.CTAS,
            atomicity=atomicity,
            temporary=temporary,
            facts=facts,
            provenance=provenance,
        )

    @classmethod
    def create_then_insert(
        cls,
        *,
        temporary: bool,
        atomicity: DdlAtomicity,
        facts: CreateFromQueryFacts,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryPlan":
        return cls(
            strategy=CreateFromQueryStrategy.CREATE_THEN_INSERT,
            atomicity=atomicity,
            temporary=temporary,
            facts=facts,
            provenance=provenance,
        )

    @classmethod
    def unsupported(
        cls,
        *,
        temporary: bool,
        reason: str,
        facts: CreateFromQueryFacts,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryPlan":
        return cls(
            strategy=CreateFromQueryStrategy.UNSUPPORTED,
            atomicity=DdlAtomicity.NONE,
            temporary=temporary,
            facts=facts,
            provenance=provenance,
            reason=reason,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return stable primitive data suitable for fixtures and cross-runtime exchange."""

        return {
            "strategy": self.strategy.value,
            "atomicity": self.atomicity.value,
            "temporary": self.temporary,
            "facts": self.facts.to_dict(),
            "provenance": [item.to_dict() for item in self.provenance],
            "reason": self.reason,
        }
