from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple, Union

from dbt.adapters.planning.create_from_query import (
    CreateFromQueryFacts,
    FormatFacts,
    RelationFacts,
)


class TableIndexStrategy(str, Enum):
    BEFORE_SWAP = "before_swap"
    AFTER_SWAP = "after_swap"
    NONE = "none"


class ExistingIndexStrategy(str, Enum):
    PRESERVE = "preserve"
    DROP_BEFORE_SWAP = "drop_before_swap"


class TableDocumentationStrategy(str, Enum):
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"


class MaterializationTransactionStrategy(str, Enum):
    EXPLICIT_COMMIT = "explicit_commit"
    ADAPTER_MANAGED = "adapter_managed"


class MaterializationHookStrategy(str, Enum):
    SPLIT = "split"
    IN_TRANSACTION = "in_transaction"


class MaterializationStatementStrategy(str, Enum):
    AUTO_BEGIN = "auto_begin"
    NO_AUTO_BEGIN = "no_auto_begin"


class MaterializationTransactionMode(str, Enum):
    TRANSACTIONAL = "transactional"
    AUTOCOMMIT = "autocommit"
    NONE = "none"


class IncompatibleRelationStrategy(str, Enum):
    """How a direct replacement treats an existing relation of another type."""

    DROP = "drop"
    DROP_ON_FULL_REFRESH = "drop_on_full_refresh"


class SnapshotHardDeletes(str, Enum):
    IGNORE = "ignore"
    INVALIDATE = "invalidate"
    NEW_RECORD = "new_record"


@dataclass(frozen=True)
class MaterializationExecutionFacts:
    """Connection capabilities relevant to lifecycle selection."""

    transaction_mode: MaterializationTransactionMode
    capabilities: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.transaction_mode, MaterializationTransactionMode):
            raise TypeError("Materialization transaction mode must be typed")
        if not isinstance(self.capabilities, tuple):
            raise TypeError("Materialization capabilities must be an immutable tuple")
        if not all(
            isinstance(capability, str) and capability.strip()
            for capability in self.capabilities
        ):
            raise ValueError("Materialization capabilities must be non-empty strings")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("Materialization capabilities cannot contain duplicates")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transaction_mode": self.transaction_mode.value,
            "capabilities": list(self.capabilities),
        }


@dataclass(frozen=True)
class ExistingRelationFacts:
    """Live physical facts that affect safe relation replacement."""

    relation: RelationFacts
    format: FormatFacts
    can_be_renamed: bool
    can_be_replaced: bool
    requires_drop_before_replace: bool
    is_shallow_clone: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.relation, RelationFacts):
            raise TypeError("Existing relation facts require typed relation facts")
        if not isinstance(self.format, FormatFacts):
            raise TypeError("Existing relation facts require typed format facts")
        for value, field_name in (
            (self.can_be_renamed, "rename capability"),
            (self.can_be_replaced, "replace capability"),
            (self.requires_drop_before_replace, "drop requirement"),
            (self.is_shallow_clone, "shallow-clone state"),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"Existing relation {field_name} must be a boolean")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "relation": self.relation.to_dict(),
            "format": self.format.to_dict(),
            "can_be_renamed": self.can_be_renamed,
            "can_be_replaced": self.can_be_replaced,
            "requires_drop_before_replace": self.requires_drop_before_replace,
            "is_shallow_clone": self.is_shallow_clone,
        }


@dataclass(frozen=True)
class TableMaterializationFacts:
    """Complete immutable inputs used to resolve a table lifecycle."""

    create: CreateFromQueryFacts
    existing: Optional[ExistingRelationFacts]
    execution: MaterializationExecutionFacts = MaterializationExecutionFacts(
        transaction_mode=MaterializationTransactionMode.TRANSACTIONAL
    )

    def __post_init__(self) -> None:
        if not isinstance(self.create, CreateFromQueryFacts):
            raise TypeError(
                "Table materialization facts require create-from-query facts"
            )
        if self.existing is not None and not isinstance(
            self.existing, ExistingRelationFacts
        ):
            raise TypeError("Existing table state must contain ExistingRelationFacts")
        if not isinstance(self.execution, MaterializationExecutionFacts):
            raise TypeError("Table materialization execution facts must be typed")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "create": self.create.to_dict(),
            "existing": self.existing.to_dict() if self.existing is not None else None,
            "execution": self.execution.to_dict(),
        }


@dataclass(frozen=True)
class ViewMaterializationFacts:
    """Resolved relation and invocation facts for one view replacement."""

    relation: TableMaterializationFacts
    full_refresh: bool

    def __post_init__(self) -> None:
        if not isinstance(self.relation, TableMaterializationFacts):
            raise TypeError("View materialization requires typed relation facts")
        if not isinstance(self.full_refresh, bool):
            raise TypeError("View full-refresh state must be a boolean")


SnapshotUniqueKey = Union[str, Tuple[str, ...]]


@dataclass(frozen=True)
class SnapshotStrategyFacts:
    """Validated SQL expressions returned by an adapter-dispatchable snapshot strategy."""

    unique_key: SnapshotUniqueKey
    updated_at: str
    row_changed: str
    scd_id: str
    hard_deletes: SnapshotHardDeletes

    def __post_init__(self) -> None:
        unique_keys = (
            (self.unique_key,) if isinstance(self.unique_key, str) else self.unique_key
        )
        if not isinstance(unique_keys, tuple) or not unique_keys:
            raise ValueError("Snapshot unique key must be a non-empty string or tuple")
        if not all(isinstance(key, str) and key.strip() for key in unique_keys):
            raise ValueError("Snapshot unique keys must be non-empty strings")
        for expression, name in (
            (self.updated_at, "updated_at"),
            (self.row_changed, "row_changed"),
            (self.scd_id, "scd_id"),
        ):
            if not isinstance(expression, str) or not expression.strip():
                raise ValueError(f"Snapshot {name} expression must be non-empty SQL")
        if not isinstance(self.hard_deletes, SnapshotHardDeletes):
            raise TypeError("Snapshot hard-delete behavior must be typed")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SnapshotStrategyFacts":
        if not isinstance(value, Mapping):
            raise TypeError("Snapshot strategy must return a mapping")
        unique_key = value.get("unique_key")
        if isinstance(unique_key, Sequence) and not isinstance(
            unique_key, (str, bytes)
        ):
            unique_key = tuple(unique_key)
        return cls(
            unique_key=unique_key,
            updated_at=value.get("updated_at"),
            row_changed=value.get("row_changed"),
            scd_id=value.get("scd_id"),
            hard_deletes=SnapshotHardDeletes(value.get("hard_deletes", "ignore")),
        )

    def to_macro_dict(self) -> Dict[str, Any]:
        unique_key: Union[str, list[str]] = (
            list(self.unique_key)
            if isinstance(self.unique_key, tuple)
            else self.unique_key
        )
        return {
            "unique_key": unique_key,
            "updated_at": self.updated_at,
            "row_changed": self.row_changed,
            "scd_id": self.scd_id,
            "hard_deletes": self.hard_deletes.value,
            "invalidate_hard_deletes": (
                self.hard_deletes == SnapshotHardDeletes.INVALIDATE
            ),
        }


@dataclass(frozen=True)
class SnapshotMaterializationFacts:
    """Complete relation and change-detection inputs for one snapshot invocation."""

    table: TableMaterializationFacts
    target_exists: bool
    strategy: SnapshotStrategyFacts

    def __post_init__(self) -> None:
        if not isinstance(self.table, TableMaterializationFacts):
            raise TypeError("Snapshot materialization requires typed table facts")
        if not isinstance(self.target_exists, bool):
            raise TypeError("Snapshot target existence must be a boolean")
        if not isinstance(self.strategy, SnapshotStrategyFacts):
            raise TypeError("Snapshot materialization requires typed strategy facts")
        if self.target_exists != (self.table.existing is not None):
            raise ValueError("Snapshot target existence must match relation facts")
