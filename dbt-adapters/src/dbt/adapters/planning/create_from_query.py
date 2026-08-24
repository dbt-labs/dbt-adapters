from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


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


class StrategyOfferStatus(str, Enum):
    """Whether one physical strategy can satisfy the resolved operation."""

    AVAILABLE = "available"
    REJECTED = "rejected"


class CreateFromQueryRenderKind(str, Enum):
    """How a resolved create-from-query plan will be rendered."""

    SQL = "sql"
    LEGACY_MACRO = "legacy_macro"


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
    catalog_provider: Optional[str] = None
    external_volume: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, CatalogBindingState):
            raise TypeError("Catalog binding state must be a CatalogBindingState")
        for value, field_name in (
            (self.integration_name, "Catalog integration name"),
            (self.catalog_type, "Catalog type"),
            (self.catalog_name, "Platform catalog name"),
            (self.catalog_database, "Catalog database"),
            (self.catalog_provider, "Catalog provider"),
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
                    self.catalog_provider,
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
                    self.catalog_provider,
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
            "catalog_provider": self.catalog_provider,
            "external_volume": self.external_volume,
        }


@dataclass(frozen=True)
class FormatFacts:
    """Resolved table and file format facts."""

    table_format: Optional[str] = None
    file_format: Optional[str] = None
    table_provider: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_optional_string(self.table_format, "Table format")
        _validate_optional_string(self.file_format, "File format")
        _validate_optional_string(self.table_provider, "Table provider")

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "table_format": self.table_format,
            "file_format": self.file_format,
            "table_provider": self.table_provider,
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
class CreateFromQueryStrategyOffer:
    """One adapter-declared physical strategy, including rejection evidence."""

    strategy: CreateFromQueryStrategy
    status: StrategyOfferStatus
    atomicity: DdlAtomicity
    provenance: Tuple[PlanProvenance, ...]
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, CreateFromQueryStrategy):
            raise TypeError("Strategy offer must use a CreateFromQueryStrategy")
        if self.strategy == CreateFromQueryStrategy.UNSUPPORTED:
            raise ValueError("Strategy offers must name a physical strategy")
        if not isinstance(self.status, StrategyOfferStatus):
            raise TypeError("Strategy offer status must be a StrategyOfferStatus")
        if not isinstance(self.atomicity, DdlAtomicity):
            raise TypeError("Strategy offer atomicity must be a DdlAtomicity")
        if not isinstance(self.provenance, tuple):
            raise TypeError("Strategy offer provenance must be an immutable tuple")
        if not self.provenance:
            raise ValueError("Strategy offer must include provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Strategy offer provenance must contain PlanProvenance")
        _validate_optional_string(self.reason, "Strategy offer rejection reason")

        if self.status == StrategyOfferStatus.AVAILABLE:
            if self.atomicity == DdlAtomicity.NONE:
                raise ValueError("Available strategy offer must promise non-none atomicity")
            if self.reason is not None:
                raise ValueError("Available strategy offer cannot include a rejection reason")
        else:
            if self.atomicity != DdlAtomicity.NONE:
                raise ValueError("Rejected strategy offer cannot promise atomicity")
            if self.reason is None:
                raise ValueError("Rejected strategy offer must include a reason")

    @classmethod
    def available(
        cls,
        *,
        strategy: CreateFromQueryStrategy,
        atomicity: DdlAtomicity,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryStrategyOffer":
        return cls(
            strategy=strategy,
            status=StrategyOfferStatus.AVAILABLE,
            atomicity=atomicity,
            provenance=provenance,
        )

    @classmethod
    def rejected(
        cls,
        *,
        strategy: CreateFromQueryStrategy,
        reason: str,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryStrategyOffer":
        return cls(
            strategy=strategy,
            status=StrategyOfferStatus.REJECTED,
            atomicity=DdlAtomicity.NONE,
            provenance=provenance,
            reason=reason,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "status": self.status.value,
            "atomicity": self.atomicity.value,
            "provenance": [item.to_dict() for item in self.provenance],
            "reason": self.reason,
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
        else:
            if self.reason is not None:
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


@dataclass(frozen=True)
class CreateFromQueryRenderArguments:
    """Late-bound values used to render an already-resolved physical plan.

    These values are deliberately separate from ``CreateFromQueryPlan``: a plan
    remains serializable, while rendered relation text and compiled SQL only
    exist in the execution runtime.
    """

    relation_sql: str
    query: str
    sql_header: Optional[str] = None
    contract_enforced: bool = False
    legacy_renderer_override: Optional[str] = None

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.relation_sql, "Create-from-query relation SQL"),
            (self.query, "Create-from-query query"),
        ):
            if not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string")
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        _validate_optional_string(self.sql_header, "Create-from-query SQL header")
        if not isinstance(self.contract_enforced, bool):
            raise TypeError("Create-from-query contract enforcement must be a bool")
        _validate_optional_string(
            self.legacy_renderer_override,
            "Create-from-query legacy renderer override",
        )


@dataclass(frozen=True)
class CreateFromQueryRenderResult:
    """Validated SQL or an explicit request to use the compatibility renderer."""

    kind: CreateFromQueryRenderKind
    provenance: Tuple[PlanProvenance, ...]
    sql: Optional[str] = None
    renderer_macro: Optional[str] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, CreateFromQueryRenderKind):
            raise TypeError("Create-from-query render kind must be a CreateFromQueryRenderKind")
        if not isinstance(self.provenance, tuple):
            raise TypeError("Create-from-query render provenance must be an immutable tuple")
        if not self.provenance:
            raise ValueError("Create-from-query render result must include provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Create-from-query render provenance must contain PlanProvenance")
        _validate_optional_string(self.sql, "Rendered create-from-query SQL")
        _validate_optional_string(self.renderer_macro, "Create-from-query fallback macro")
        _validate_optional_string(self.reason, "Create-from-query fallback reason")

        if self.kind == CreateFromQueryRenderKind.SQL:
            if self.sql is None:
                raise ValueError("SQL render result must contain SQL")
            if self.renderer_macro is not None or self.reason is not None:
                raise ValueError("SQL render result cannot contain fallback fields")
        else:
            if self.sql is not None:
                raise ValueError("Legacy macro render result cannot contain SQL")
            if self.renderer_macro is None or self.reason is None:
                raise ValueError("Legacy macro render result requires a macro and reason")

    @classmethod
    def rendered_sql(
        cls, sql: str, provenance: Tuple[PlanProvenance, ...]
    ) -> "CreateFromQueryRenderResult":
        return cls(kind=CreateFromQueryRenderKind.SQL, sql=sql, provenance=provenance)

    @classmethod
    def legacy_macro(
        cls,
        *,
        renderer_macro: str,
        reason: str,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "CreateFromQueryRenderResult":
        return cls(
            kind=CreateFromQueryRenderKind.LEGACY_MACRO,
            renderer_macro=renderer_macro,
            reason=reason,
            provenance=provenance,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind.value,
            "sql": self.sql,
            "renderer_macro": self.renderer_macro,
            "reason": self.reason,
            "provenance": [item.to_dict() for item in self.provenance],
        }


def resolve_create_from_query_offers(
    *,
    temporary: bool,
    facts: CreateFromQueryFacts,
    offers: Iterable[CreateFromQueryStrategyOffer],
) -> CreateFromQueryPlan:
    """Select the first available physical strategy or return an unsupported plan."""

    resolved_offers = tuple(offers)
    if not resolved_offers:
        reason = "Adapter declared no create-from-query strategies"
        return CreateFromQueryPlan.unsupported(
            temporary=temporary,
            facts=facts,
            reason=reason,
            provenance=(
                PlanProvenance(
                    rule="create_from_query.offers.empty",
                    detail=reason,
                ),
            ),
        )
    if not all(isinstance(offer, CreateFromQueryStrategyOffer) for offer in resolved_offers):
        raise TypeError("Create-from-query offers must contain strategy offers")

    rejected_provenance: List[PlanProvenance] = []
    for offer in resolved_offers:
        if offer.status == StrategyOfferStatus.AVAILABLE:
            return CreateFromQueryPlan(
                strategy=offer.strategy,
                atomicity=offer.atomicity,
                temporary=temporary,
                facts=facts,
                provenance=tuple(rejected_provenance) + offer.provenance,
            )
        rejected_provenance.extend(offer.provenance)

    reasons = tuple(offer.reason for offer in resolved_offers if offer.reason is not None)
    reason = "; ".join(reasons) or "All create-from-query strategies were rejected"
    provenance = tuple(item for offer in resolved_offers for item in offer.provenance)
    return CreateFromQueryPlan.unsupported(
        temporary=temporary,
        facts=facts,
        reason=reason,
        provenance=provenance,
    )
