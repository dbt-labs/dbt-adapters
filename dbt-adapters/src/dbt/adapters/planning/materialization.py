from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from dbt.adapters.planning.create_from_query import PlanProvenance


class TableReplacementStrategy(str, Enum):
    """Closed table replacement lifecycles understood by dbt-core."""

    STAGE_AND_SWAP = "stage_and_swap"
    DIRECT_REPLACE = "direct_replace"


class TableIndexStrategy(str, Enum):
    """Point in the replacement lifecycle where indexes are applied."""

    BEFORE_SWAP = "before_swap"
    AFTER_SWAP = "after_swap"
    NONE = "none"


class ExistingIndexStrategy(str, Enum):
    """Preparation required before replacing an existing relation."""

    PRESERVE = "preserve"
    DROP_BEFORE_SWAP = "drop_before_swap"


class TableDocumentationStrategy(str, Enum):
    """Transaction boundary relative to persisted relation documentation."""

    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"


class MaterializationTransactionStrategy(str, Enum):
    """Owner of materialization transaction boundaries."""

    EXPLICIT_COMMIT = "explicit_commit"
    ADAPTER_MANAGED = "adapter_managed"


class MaterializationHookStrategy(str, Enum):
    """How hooks participate in the materialization transaction."""

    SPLIT = "split"
    IN_TRANSACTION = "in_transaction"


class MaterializationStatementStrategy(str, Enum):
    """Whether statement execution should open a transaction automatically."""

    AUTO_BEGIN = "auto_begin"
    NO_AUTO_BEGIN = "no_auto_begin"


@dataclass(frozen=True)
class TableLifecyclePlan:
    """Serializable table lifecycle selected before DDL rendering begins."""

    replacement: TableReplacementStrategy
    indexes: TableIndexStrategy
    existing_indexes: ExistingIndexStrategy
    documentation: TableDocumentationStrategy
    transaction: MaterializationTransactionStrategy
    hooks: MaterializationHookStrategy
    statement: MaterializationStatementStrategy
    provenance: Tuple[PlanProvenance, ...]
    setup_macro: Optional[str] = None
    teardown_macro: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.replacement, TableReplacementStrategy):
            raise TypeError("Table lifecycle replacement must be typed")
        if not isinstance(self.indexes, TableIndexStrategy):
            raise TypeError("Table lifecycle index strategy must be typed")
        if not isinstance(self.existing_indexes, ExistingIndexStrategy):
            raise TypeError("Table lifecycle existing-index strategy must be typed")
        if not isinstance(self.documentation, TableDocumentationStrategy):
            raise TypeError("Table lifecycle documentation strategy must be typed")
        if not isinstance(self.transaction, MaterializationTransactionStrategy):
            raise TypeError("Table lifecycle transaction strategy must be typed")
        if not isinstance(self.hooks, MaterializationHookStrategy):
            raise TypeError("Table lifecycle hook strategy must be typed")
        if not isinstance(self.statement, MaterializationStatementStrategy):
            raise TypeError("Table lifecycle statement strategy must be typed")
        if not isinstance(self.provenance, tuple):
            raise TypeError("Table lifecycle provenance must be an immutable tuple")
        if not self.provenance:
            raise ValueError("Table lifecycle plan must include provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Table lifecycle provenance must contain PlanProvenance")

        for value, field_name in (
            (self.setup_macro, "setup macro"),
            (self.teardown_macro, "teardown macro"),
        ):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(
                    f"Table lifecycle {field_name} must be a non-empty string"
                )
        if (self.setup_macro is None) != (self.teardown_macro is None):
            raise ValueError("Table lifecycle setup and teardown macros must be paired")
        if (
            self.documentation == TableDocumentationStrategy.AFTER_COMMIT
            and self.transaction != MaterializationTransactionStrategy.EXPLICIT_COMMIT
        ):
            raise ValueError(
                "Post-commit documentation requires explicit transaction control"
            )
        if (
            self.replacement == TableReplacementStrategy.DIRECT_REPLACE
            and self.indexes == TableIndexStrategy.BEFORE_SWAP
        ):
            raise ValueError("Direct replacement cannot create indexes before a swap")
        if (
            self.replacement == TableReplacementStrategy.DIRECT_REPLACE
            and self.existing_indexes == ExistingIndexStrategy.DROP_BEFORE_SWAP
        ):
            raise ValueError(
                "Direct replacement has no swap boundary for existing indexes"
            )

    @classmethod
    def stage_and_swap(
        cls,
        *,
        indexes: TableIndexStrategy = TableIndexStrategy.BEFORE_SWAP,
        existing_indexes: ExistingIndexStrategy = ExistingIndexStrategy.PRESERVE,
        documentation: TableDocumentationStrategy = TableDocumentationStrategy.BEFORE_COMMIT,
        statement: MaterializationStatementStrategy = MaterializationStatementStrategy.AUTO_BEGIN,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "TableLifecyclePlan":
        return cls(
            replacement=TableReplacementStrategy.STAGE_AND_SWAP,
            indexes=indexes,
            existing_indexes=existing_indexes,
            documentation=documentation,
            transaction=MaterializationTransactionStrategy.EXPLICIT_COMMIT,
            hooks=MaterializationHookStrategy.SPLIT,
            statement=statement,
            provenance=provenance,
        )

    @classmethod
    def direct_replace(
        cls,
        *,
        setup_macro: Optional[str] = None,
        teardown_macro: Optional[str] = None,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "TableLifecyclePlan":
        return cls(
            replacement=TableReplacementStrategy.DIRECT_REPLACE,
            indexes=TableIndexStrategy.NONE,
            existing_indexes=ExistingIndexStrategy.PRESERVE,
            documentation=TableDocumentationStrategy.BEFORE_COMMIT,
            transaction=MaterializationTransactionStrategy.ADAPTER_MANAGED,
            hooks=MaterializationHookStrategy.IN_TRANSACTION,
            statement=MaterializationStatementStrategy.AUTO_BEGIN,
            setup_macro=setup_macro,
            teardown_macro=teardown_macro,
            provenance=provenance,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "replacement": self.replacement.value,
            "indexes": self.indexes.value,
            "existing_indexes": self.existing_indexes.value,
            "documentation": self.documentation.value,
            "transaction": self.transaction.value,
            "hooks": self.hooks.value,
            "statement": self.statement.value,
            "setup_macro": self.setup_macro,
            "teardown_macro": self.teardown_macro,
            "provenance": [item.to_dict() for item in self.provenance],
        }
