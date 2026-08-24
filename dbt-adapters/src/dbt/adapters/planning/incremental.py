from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from dbt.adapters.planning.create_from_query import (
    DdlAtomicity,
    PlanProvenance,
    StrategyOfferStatus,
)
from dbt.adapters.planning.materialization import (
    MaterializationOperation,
    TableMaterializationFacts,
)


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


class IncrementalTempRelationType(str, Enum):
    """Physical staging relations understood by incremental planners."""

    VIEW = "view"
    TABLE = "table"
    TRANSIENT = "transient"


class IncrementalUniqueKeyRequirement(str, Enum):
    """How a mutation strategy treats a configured unique key."""

    IGNORED = "ignored"
    OPTIONAL = "optional"
    REQUIRED = "required"


class IncrementalSourceConsistency(str, Enum):
    """Whether a renderer must reuse one stable evaluation of the model query."""

    SINGLE_EVALUATION = "single_evaluation"
    STABLE_REUSE = "stable_reuse"


class IncrementalCatalogStaging(str, Enum):
    """Catalog-imposed staging scope available to an incremental operation."""

    STANDARD = "standard"
    PERMANENT_TABLE_ONLY = "permanent_table_only"


@dataclass(frozen=True)
class IncrementalMutationFacts:
    """Typed, renderer-independent inputs used to choose an incremental strategy."""

    requested_strategy: str
    language: str
    unique_key_present: bool
    requested_temp_relation_type: Optional[str] = None
    catalog_staging: IncrementalCatalogStaging = IncrementalCatalogStaging.STANDARD

    def __post_init__(self) -> None:
        if not isinstance(self.requested_strategy, str) or not self.requested_strategy.strip():
            raise ValueError("Incremental facts require a non-empty requested strategy")
        if not isinstance(self.language, str) or not self.language.strip():
            raise ValueError("Incremental facts require a non-empty model language")
        if not isinstance(self.unique_key_present, bool):
            raise TypeError("Incremental unique-key presence must be a boolean")
        if self.requested_temp_relation_type is not None and (
            not isinstance(self.requested_temp_relation_type, str)
            or not self.requested_temp_relation_type.strip()
        ):
            raise ValueError("Requested incremental temporary relation type cannot be empty")
        if not isinstance(self.catalog_staging, IncrementalCatalogStaging):
            raise TypeError("Incremental catalog staging must be typed")


@dataclass(frozen=True)
class IncrementalStrategyRequirements:
    """Declarative preconditions attached to an incremental strategy offer."""

    unique_key: IncrementalUniqueKeyRequirement
    source_consistency: IncrementalSourceConsistency
    allowed_temp_relation_types: Tuple[IncrementalTempRelationType, ...] = ()
    default_temp_relation_type: Optional[IncrementalTempRelationType] = None
    supported_languages: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.unique_key, IncrementalUniqueKeyRequirement):
            raise TypeError("Incremental unique-key requirement must be typed")
        if not isinstance(self.source_consistency, IncrementalSourceConsistency):
            raise TypeError("Incremental source-consistency requirement must be typed")
        if not isinstance(self.allowed_temp_relation_types, tuple):
            raise TypeError("Allowed incremental temp relation types must be an immutable tuple")
        if not all(
            isinstance(item, IncrementalTempRelationType)
            for item in self.allowed_temp_relation_types
        ):
            raise TypeError("Allowed incremental temp relation types must be typed")
        if len(set(self.allowed_temp_relation_types)) != len(self.allowed_temp_relation_types):
            raise ValueError("Allowed incremental temp relation types cannot contain duplicates")
        if self.default_temp_relation_type is not None:
            if not isinstance(self.default_temp_relation_type, IncrementalTempRelationType):
                raise TypeError("Default incremental temp relation type must be typed")
            if self.default_temp_relation_type not in self.allowed_temp_relation_types:
                raise ValueError("Default incremental temp relation type must be allowed")
        if not isinstance(self.supported_languages, tuple):
            raise TypeError("Supported incremental languages must be an immutable tuple")
        if not all(
            isinstance(language, str) and language.strip() for language in self.supported_languages
        ):
            raise ValueError("Supported incremental languages must be non-empty strings")


@dataclass(frozen=True)
class IncrementalMutationStrategyOffer:
    """One adapter-provided incremental strategy candidate and its requirements."""

    status: StrategyOfferStatus
    strategy: IncrementalMutationStrategy
    renderer_macro: Optional[str]
    atomicity: DdlAtomicity
    requirements: IncrementalStrategyRequirements
    provenance: Tuple[PlanProvenance, ...]
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, StrategyOfferStatus):
            raise TypeError("Incremental offer status must be typed")
        if not isinstance(self.strategy, IncrementalMutationStrategy):
            raise TypeError("Incremental offer strategy must be typed")
        if not isinstance(self.atomicity, DdlAtomicity):
            raise TypeError("Incremental offer atomicity must be typed")
        if not isinstance(self.requirements, IncrementalStrategyRequirements):
            raise TypeError("Incremental offer requirements must be typed")
        if not isinstance(self.provenance, tuple) or not self.provenance:
            raise ValueError("Incremental offer must include immutable provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Incremental offer provenance must contain PlanProvenance")
        if self.status == StrategyOfferStatus.AVAILABLE:
            if not self.renderer_macro or not self.renderer_macro.strip():
                raise ValueError("Available incremental offer must select a renderer macro")
            if self.reason is not None:
                raise ValueError("Available incremental offer cannot include a rejection reason")
        else:
            if self.renderer_macro is not None:
                raise ValueError("Rejected incremental offer cannot select a renderer macro")
            if self.atomicity != DdlAtomicity.NONE:
                raise ValueError("Rejected incremental offer cannot promise atomicity")
            if not self.reason or not self.reason.strip():
                raise ValueError("Rejected incremental offer must include a reason")

    @classmethod
    def available(
        cls,
        *,
        strategy: IncrementalMutationStrategy,
        renderer_macro: str,
        atomicity: DdlAtomicity,
        requirements: IncrementalStrategyRequirements,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "IncrementalMutationStrategyOffer":
        return cls(
            status=StrategyOfferStatus.AVAILABLE,
            strategy=strategy,
            renderer_macro=renderer_macro,
            atomicity=atomicity,
            requirements=requirements,
            provenance=provenance,
        )

    @classmethod
    def rejected(
        cls,
        *,
        strategy: IncrementalMutationStrategy,
        reason: str,
        requirements: IncrementalStrategyRequirements,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "IncrementalMutationStrategyOffer":
        return cls(
            status=StrategyOfferStatus.REJECTED,
            strategy=strategy,
            renderer_macro=None,
            atomicity=DdlAtomicity.NONE,
            requirements=requirements,
            provenance=provenance,
            reason=reason,
        )


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
    adapter_arguments: Tuple[Tuple[str, Any], ...] = ()

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
        if not isinstance(self.adapter_arguments, tuple):
            raise TypeError("Incremental adapter arguments must be an immutable tuple")
        reserved = {
            "target_relation",
            "temp_relation",
            "unique_key",
            "dest_columns",
            "incremental_predicates",
        }
        keys = []
        for item in self.adapter_arguments:
            if not isinstance(item, tuple) or len(item) != 2:
                raise TypeError("Incremental adapter arguments must contain key-value tuples")
            key, _ = item
            if not isinstance(key, str) or not key.strip():
                raise ValueError("Incremental adapter argument names must be non-empty strings")
            if key in reserved:
                raise ValueError(f"Incremental adapter argument '{key}' is reserved")
            keys.append(key)
        if len(set(keys)) != len(keys):
            raise ValueError("Incremental adapter argument names must be unique")

    @classmethod
    def from_values(
        cls,
        *,
        target_relation: Any,
        temp_relation: Any,
        unique_key: Optional[Union[str, Sequence[str]]],
        dest_columns: Iterable[Any],
        incremental_predicates: Optional[Sequence[str]],
        adapter_arguments: Optional[Mapping[str, Any]] = None,
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
            adapter_arguments=tuple((adapter_arguments or {}).items()),
        )

    def to_macro_dict(self) -> Dict[str, Any]:
        unique_key: Optional[Union[str, List[str]]]
        if isinstance(self.unique_key, tuple):
            unique_key = list(self.unique_key)
        else:
            unique_key = self.unique_key

        result = {
            "target_relation": self.target_relation,
            "temp_relation": self.temp_relation,
            "unique_key": unique_key,
            "dest_columns": list(self.dest_columns),
            "incremental_predicates": (
                None if self.incremental_predicates is None else list(self.incremental_predicates)
            ),
        }
        result.update(self.adapter_arguments)
        return result


@dataclass(frozen=True)
class IncrementalMutationPlan:
    """Validated selection of one incremental mutation renderer."""

    requested_strategy: str
    strategy: IncrementalMutationStrategy
    renderer_macro: Optional[str]
    atomicity: DdlAtomicity
    provenance: Tuple[PlanProvenance, ...]
    requirements: Optional[IncrementalStrategyRequirements] = None
    temp_relation_type: Optional[IncrementalTempRelationType] = None
    catalog_staging: IncrementalCatalogStaging = IncrementalCatalogStaging.STANDARD
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
        if self.requirements is not None and not isinstance(
            self.requirements, IncrementalStrategyRequirements
        ):
            raise TypeError("Incremental plan requirements must be typed")
        if self.temp_relation_type is not None and not isinstance(
            self.temp_relation_type, IncrementalTempRelationType
        ):
            raise TypeError("Incremental plan temp relation type must be typed")
        if not isinstance(self.catalog_staging, IncrementalCatalogStaging):
            raise TypeError("Incremental plan catalog staging must be typed")
        if self.reason is not None and not isinstance(self.reason, str):
            raise TypeError("Incremental plan reason must be a string")

        if self.strategy == IncrementalMutationStrategy.UNSUPPORTED:
            if self.renderer_macro is not None:
                raise ValueError("Unsupported incremental plan cannot select a renderer macro")
            if self.atomicity != DdlAtomicity.NONE:
                raise ValueError("Unsupported incremental plan cannot promise atomicity")
            if self.temp_relation_type is not None:
                raise ValueError("Unsupported incremental plan cannot select a temp relation type")
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
            "requirements": (
                None
                if self.requirements is None
                else {
                    "unique_key": self.requirements.unique_key.value,
                    "source_consistency": self.requirements.source_consistency.value,
                    "allowed_temp_relation_types": [
                        item.value for item in self.requirements.allowed_temp_relation_types
                    ],
                    "default_temp_relation_type": (
                        None
                        if self.requirements.default_temp_relation_type is None
                        else self.requirements.default_temp_relation_type.value
                    ),
                    "supported_languages": list(self.requirements.supported_languages),
                }
            ),
            "temp_relation_type": (
                None if self.temp_relation_type is None else self.temp_relation_type.value
            ),
            "catalog_staging": self.catalog_staging.value,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class IncrementalLifecyclePlan:
    """Resolved live facts and ordered operations for one incremental run."""

    mutation: IncrementalMutationPlan
    schema_change: IncrementalSchemaChangePlan
    facts: TableMaterializationFacts
    full_refresh: bool
    operations: Tuple[MaterializationOperation, ...]
    provenance: Tuple[PlanProvenance, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.mutation, IncrementalMutationPlan):
            raise TypeError("Incremental lifecycle requires a typed mutation plan")
        if not isinstance(self.schema_change, IncrementalSchemaChangePlan):
            raise TypeError("Incremental lifecycle requires a typed schema-change plan")
        if not isinstance(self.facts, TableMaterializationFacts):
            raise TypeError("Incremental lifecycle requires typed materialization facts")
        if not isinstance(self.full_refresh, bool):
            raise TypeError("Incremental lifecycle full-refresh state must be a boolean")
        if not isinstance(self.operations, tuple) or not self.operations:
            raise ValueError("Incremental lifecycle requires immutable ordered operations")
        if not all(isinstance(item, MaterializationOperation) for item in self.operations):
            raise TypeError("Incremental lifecycle operations must be typed")
        if not isinstance(self.provenance, tuple) or not self.provenance:
            raise ValueError("Incremental lifecycle requires immutable provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Incremental lifecycle provenance must be typed")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mutation": self.mutation.to_dict(),
            "schema_change": self.schema_change.to_dict(),
            "facts": self.facts.to_dict(),
            "full_refresh": self.full_refresh,
            "operations": [item.to_dict() for item in self.operations],
            "provenance": [item.to_dict() for item in self.provenance],
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
    facts = IncrementalMutationFacts(
        requested_strategy=requested,
        language="sql",
        unique_key_present=False,
    )
    valid = frozenset(valid_strategies) | {"default"}
    builtin = frozenset(builtin_strategies)
    strategy = _BUILTIN_STRATEGIES.get(requested, IncrementalMutationStrategy.CUSTOM)
    requirements = IncrementalStrategyRequirements(
        unique_key=IncrementalUniqueKeyRequirement.OPTIONAL,
        source_consistency=IncrementalSourceConsistency.SINGLE_EVALUATION,
    )

    if requested in builtin and requested not in valid:
        reason = f"The incremental strategy '{requested}' is not valid for this adapter"
        offer = IncrementalMutationStrategyOffer.rejected(
            strategy=strategy,
            requirements=requirements,
            reason=reason,
            provenance=(
                PlanProvenance(
                    rule="incremental.requested_strategy.unsupported",
                    detail=reason,
                ),
            ),
        )
        return resolve_incremental_mutation_offers(facts=facts, offers=(offer,))

    renderer_macro = incremental_renderer_macro(requested)
    offer = IncrementalMutationStrategyOffer.available(
        strategy=strategy,
        renderer_macro=renderer_macro,
        atomicity=DdlAtomicity.UNKNOWN,
        requirements=requirements,
        provenance=(
            PlanProvenance(
                rule=f"incremental.requested_strategy.{strategy.value}",
                detail=f"Requested strategy '{requested}' resolved to '{renderer_macro}'",
            ),
        ),
    )
    return resolve_incremental_mutation_offers(facts=facts, offers=(offer,))


def incremental_strategy(requested_strategy: str) -> IncrementalMutationStrategy:
    return _BUILTIN_STRATEGIES.get(requested_strategy, IncrementalMutationStrategy.CUSTOM)


def incremental_renderer_macro(requested_strategy: str) -> str:
    return f"get_incremental_{requested_strategy.replace('+', '_')}_sql"


def resolve_incremental_mutation_offers(
    *,
    facts: IncrementalMutationFacts,
    offers: Sequence[IncrementalMutationStrategyOffer],
) -> IncrementalMutationPlan:
    """Select the first offer whose declarative requirements match the facts."""

    if not offers:
        raise ValueError("Incremental mutation resolution requires at least one offer")

    rejected_reasons: List[str] = []
    rejected_provenance: List[PlanProvenance] = []
    last_requirements: Optional[IncrementalStrategyRequirements] = None
    for offer in offers:
        last_requirements = offer.requirements
        if offer.status == StrategyOfferStatus.REJECTED:
            rejected_reasons.append(offer.reason or "Incremental strategy offer was rejected")
            rejected_provenance.extend(offer.provenance)
            continue

        requirement_error = _incremental_requirement_error(facts, offer.requirements)
        if requirement_error is not None:
            rejected_reasons.append(requirement_error)
            rejected_provenance.extend(offer.provenance)
            rejected_provenance.append(
                PlanProvenance(
                    rule="incremental.offer.requirements.rejected",
                    detail=requirement_error,
                )
            )
            continue

        requested_temp_relation_type = None
        if facts.requested_temp_relation_type is not None:
            requested_temp_relation_type = IncrementalTempRelationType(
                facts.requested_temp_relation_type
            )
        temp_relation_type = (
            requested_temp_relation_type or offer.requirements.default_temp_relation_type
        )
        return IncrementalMutationPlan(
            requested_strategy=facts.requested_strategy,
            strategy=offer.strategy,
            renderer_macro=offer.renderer_macro,
            atomicity=offer.atomicity,
            requirements=offer.requirements,
            temp_relation_type=temp_relation_type,
            catalog_staging=facts.catalog_staging,
            provenance=tuple(rejected_provenance) + offer.provenance,
        )

    reason = "; ".join(dict.fromkeys(rejected_reasons))
    return IncrementalMutationPlan(
        requested_strategy=facts.requested_strategy,
        strategy=IncrementalMutationStrategy.UNSUPPORTED,
        renderer_macro=None,
        atomicity=DdlAtomicity.NONE,
        requirements=last_requirements,
        catalog_staging=facts.catalog_staging,
        provenance=tuple(rejected_provenance),
        reason=reason,
    )


def _incremental_requirement_error(
    facts: IncrementalMutationFacts,
    requirements: IncrementalStrategyRequirements,
) -> Optional[str]:
    if requirements.supported_languages and facts.language not in requirements.supported_languages:
        return (
            f"Incremental strategy '{facts.requested_strategy}' does not support "
            f"language '{facts.language}'"
        )
    if (
        requirements.unique_key == IncrementalUniqueKeyRequirement.REQUIRED
        and not facts.unique_key_present
    ):
        return f"Incremental strategy '{facts.requested_strategy}' requires a unique key"
    if facts.requested_temp_relation_type is not None:
        try:
            requested_temp_relation_type = IncrementalTempRelationType(
                facts.requested_temp_relation_type
            )
        except ValueError:
            return (
                f"Unknown incremental temporary relation type "
                f"'{facts.requested_temp_relation_type}'"
            )
        if requested_temp_relation_type not in requirements.allowed_temp_relation_types:
            allowed = ", ".join(item.value for item in requirements.allowed_temp_relation_types)
            return (
                f"Incremental strategy '{facts.requested_strategy}' only supports temporary "
                f"relation types [{allowed}], but '{requested_temp_relation_type.value}' "
                "was requested"
            )
    return None
