from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from dbt.adapters.planning.create_from_query import (
    CreateFromQueryFacts,
    FormatFacts,
    PlanProvenance,
    RelationFacts,
)


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


class MaterializationRelationRole(str, Enum):
    """Symbolic relation bindings used by a materialization program."""

    EXISTING = "existing"
    TARGET = "target"
    INTERMEDIATE = "intermediate"
    BACKUP = "backup"
    STAGING = "staging"
    TEMP = "temp"


class MaterializationOperationKind(str, Enum):
    """Closed operation vocabulary understood by the Python executor."""

    INVOKE_CALLBACK = "invoke_callback"
    DROP_RELATION_IF_EXISTS = "drop_relation_if_exists"
    RUN_HOOKS = "run_hooks"
    CREATE_FROM_QUERY = "create_from_query"
    CREATE_FROM_RELATION = "create_from_relation"
    EXPAND_TARGET_COLUMN_TYPES = "expand_target_column_types"
    PROCESS_SCHEMA_CHANGES = "process_schema_changes"
    PROCESS_CONFIG_CHANGES = "process_config_changes"
    CAPTURE_CONFIG_CHANGES = "capture_config_changes"
    APPLY_CONFIG_CHANGES = "apply_config_changes"
    SET_INCREMENTAL_OVERWRITE_MODE = "set_incremental_overwrite_mode"
    EXECUTE_INCREMENTAL_MUTATION = "execute_incremental_mutation"
    RENAME_RELATION = "rename_relation"
    CREATE_INDEXES = "create_indexes"
    APPLY_GRANTS = "apply_grants"
    APPLY_TAGS = "apply_tags"
    APPLY_COLUMN_TAGS = "apply_column_tags"
    PERSIST_DOCUMENTATION = "persist_documentation"
    PERSIST_CONSTRAINTS = "persist_constraints"
    OPTIMIZE = "optimize"
    COMMIT = "commit"


class MaterializationTransactionMode(str, Enum):
    """Effective transaction behavior of the active adapter connection."""

    TRANSACTIONAL = "transactional"
    AUTOCOMMIT = "autocommit"
    NONE = "none"


@dataclass(frozen=True)
class MaterializationExecutionFacts:
    """Connection and named runtime capabilities relevant to lifecycle execution."""

    transaction_mode: MaterializationTransactionMode
    capabilities: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.transaction_mode, MaterializationTransactionMode):
            raise TypeError("Materialization transaction mode must be typed")
        if not isinstance(self.capabilities, tuple):
            raise TypeError("Materialization capabilities must be an immutable tuple")
        if not all(
            isinstance(capability, str) and capability.strip() for capability in self.capabilities
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
            raise TypeError("Table materialization facts require create-from-query facts")
        if self.existing is not None and not isinstance(self.existing, ExistingRelationFacts):
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
class MaterializationOperation:
    """One serializable, ordered mutation in a materialization program."""

    kind: MaterializationOperationKind
    relation: Optional[MaterializationRelationRole] = None
    source: Optional[MaterializationRelationRole] = None
    destination: Optional[MaterializationRelationRole] = None
    name: Optional[str] = None
    inside_transaction: Optional[bool] = None
    temporary: Optional[bool] = None
    auto_begin: Optional[bool] = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, MaterializationOperationKind):
            raise TypeError("Materialization operation kind must be typed")
        for value, field_name in (
            (self.relation, "relation"),
            (self.source, "source"),
            (self.destination, "destination"),
        ):
            if value is not None and not isinstance(value, MaterializationRelationRole):
                raise TypeError(f"Materialization operation {field_name} must be typed")
        if self.name is not None and (not isinstance(self.name, str) or not self.name.strip()):
            raise ValueError("Materialization operation name must be a non-empty string")
        for value, field_name in (
            (self.inside_transaction, "inside_transaction"),
            (self.temporary, "temporary"),
            (self.auto_begin, "auto_begin"),
        ):
            if value is not None and not isinstance(value, bool):
                raise TypeError(f"Materialization operation {field_name} must be a boolean")

        relation_required = {
            MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
            MaterializationOperationKind.CREATE_FROM_QUERY,
            MaterializationOperationKind.CREATE_FROM_RELATION,
            MaterializationOperationKind.EXPAND_TARGET_COLUMN_TYPES,
            MaterializationOperationKind.PROCESS_SCHEMA_CHANGES,
            MaterializationOperationKind.PROCESS_CONFIG_CHANGES,
            MaterializationOperationKind.CAPTURE_CONFIG_CHANGES,
            MaterializationOperationKind.APPLY_CONFIG_CHANGES,
            MaterializationOperationKind.EXECUTE_INCREMENTAL_MUTATION,
            MaterializationOperationKind.RENAME_RELATION,
            MaterializationOperationKind.CREATE_INDEXES,
            MaterializationOperationKind.APPLY_GRANTS,
            MaterializationOperationKind.APPLY_TAGS,
            MaterializationOperationKind.APPLY_COLUMN_TAGS,
            MaterializationOperationKind.PERSIST_DOCUMENTATION,
            MaterializationOperationKind.PERSIST_CONSTRAINTS,
            MaterializationOperationKind.OPTIMIZE,
        }
        if self.kind in relation_required and self.relation is None:
            raise ValueError(f"{self.kind.value} operation requires a relation role")
        if (
            self.kind
            in {
                MaterializationOperationKind.CREATE_FROM_RELATION,
                MaterializationOperationKind.EXPAND_TARGET_COLUMN_TYPES,
                MaterializationOperationKind.PROCESS_SCHEMA_CHANGES,
                MaterializationOperationKind.PROCESS_CONFIG_CHANGES,
                MaterializationOperationKind.APPLY_CONFIG_CHANGES,
                MaterializationOperationKind.EXECUTE_INCREMENTAL_MUTATION,
            }
            and self.source is None
        ):
            raise ValueError(f"{self.kind.value} operation requires a source role")
        if self.kind == MaterializationOperationKind.RENAME_RELATION and self.destination is None:
            raise ValueError("rename_relation operation requires a destination role")
        if (
            self.kind == MaterializationOperationKind.SET_INCREMENTAL_OVERWRITE_MODE
            and self.name not in {"DYNAMIC", "STATIC"}
        ):
            raise ValueError("set_incremental_overwrite_mode requires DYNAMIC or STATIC")
        if (
            self.kind
            in {
                MaterializationOperationKind.INVOKE_CALLBACK,
                MaterializationOperationKind.RUN_HOOKS,
            }
            and self.name is None
        ):
            raise ValueError(f"{self.kind.value} operation requires a name")
        if self.kind == MaterializationOperationKind.RUN_HOOKS and self.inside_transaction is None:
            raise ValueError("run_hooks operation requires transaction placement")
        if self.kind == MaterializationOperationKind.CREATE_FROM_QUERY:
            if self.temporary is None or self.auto_begin is None:
                raise ValueError(
                    "create_from_query operation requires temporary and auto_begin policies"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind.value,
            "relation": self.relation.value if self.relation is not None else None,
            "source": self.source.value if self.source is not None else None,
            "destination": (self.destination.value if self.destination is not None else None),
            "name": self.name,
            "inside_transaction": self.inside_transaction,
            "temporary": self.temporary,
            "auto_begin": self.auto_begin,
        }


@dataclass(frozen=True)
class TableLifecyclePlan:
    """Serializable table lifecycle resolved before DDL rendering begins."""

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
    facts: Optional[TableMaterializationFacts] = None
    operations: Tuple[MaterializationOperation, ...] = ()

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
        if self.facts is not None and not isinstance(self.facts, TableMaterializationFacts):
            raise TypeError("Table lifecycle facts must be typed")
        if not isinstance(self.operations, tuple):
            raise TypeError("Table lifecycle operations must be an immutable tuple")
        if not all(isinstance(item, MaterializationOperation) for item in self.operations):
            raise TypeError("Table lifecycle operations must be typed")
        if (self.facts is None) != (not self.operations):
            raise ValueError(
                "Resolved table lifecycle facts and operations must be supplied together"
            )

        for value, field_name in (
            (self.setup_macro, "setup macro"),
            (self.teardown_macro, "teardown macro"),
        ):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"Table lifecycle {field_name} must be a non-empty string")
        if (self.setup_macro is None) != (self.teardown_macro is None):
            raise ValueError("Table lifecycle setup and teardown macros must be paired")
        if (
            self.documentation == TableDocumentationStrategy.AFTER_COMMIT
            and self.transaction != MaterializationTransactionStrategy.EXPLICIT_COMMIT
        ):
            raise ValueError("Post-commit documentation requires explicit transaction control")
        if (
            self.replacement == TableReplacementStrategy.DIRECT_REPLACE
            and self.indexes == TableIndexStrategy.BEFORE_SWAP
        ):
            raise ValueError("Direct replacement cannot create indexes before a swap")
        if (
            self.replacement == TableReplacementStrategy.DIRECT_REPLACE
            and self.existing_indexes == ExistingIndexStrategy.DROP_BEFORE_SWAP
        ):
            raise ValueError("Direct replacement has no swap boundary for existing indexes")

    @property
    def is_resolved(self) -> bool:
        return self.facts is not None

    def resolve(
        self,
        *,
        facts: TableMaterializationFacts,
        operations: Tuple[MaterializationOperation, ...],
        provenance: Tuple[PlanProvenance, ...] = (),
    ) -> "TableLifecyclePlan":
        if not operations:
            raise ValueError("Resolved table lifecycle requires at least one operation")
        return replace(
            self,
            facts=facts,
            operations=operations,
            provenance=self.provenance + provenance,
        )

    @classmethod
    def stage_and_swap(
        cls,
        *,
        indexes: TableIndexStrategy = TableIndexStrategy.BEFORE_SWAP,
        existing_indexes: ExistingIndexStrategy = ExistingIndexStrategy.PRESERVE,
        documentation: TableDocumentationStrategy = TableDocumentationStrategy.BEFORE_COMMIT,
        transaction: MaterializationTransactionStrategy = MaterializationTransactionStrategy.EXPLICIT_COMMIT,
        hooks: MaterializationHookStrategy = MaterializationHookStrategy.SPLIT,
        statement: MaterializationStatementStrategy = MaterializationStatementStrategy.AUTO_BEGIN,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "TableLifecyclePlan":
        return cls(
            replacement=TableReplacementStrategy.STAGE_AND_SWAP,
            indexes=indexes,
            existing_indexes=existing_indexes,
            documentation=documentation,
            transaction=transaction,
            hooks=hooks,
            statement=statement,
            provenance=provenance,
        )

    @classmethod
    def direct_replace(
        cls,
        *,
        setup_macro: Optional[str] = None,
        teardown_macro: Optional[str] = None,
        transaction: MaterializationTransactionStrategy = MaterializationTransactionStrategy.ADAPTER_MANAGED,
        hooks: MaterializationHookStrategy = MaterializationHookStrategy.IN_TRANSACTION,
        statement: MaterializationStatementStrategy = MaterializationStatementStrategy.AUTO_BEGIN,
        provenance: Tuple[PlanProvenance, ...],
    ) -> "TableLifecyclePlan":
        return cls(
            replacement=TableReplacementStrategy.DIRECT_REPLACE,
            indexes=TableIndexStrategy.NONE,
            existing_indexes=ExistingIndexStrategy.PRESERVE,
            documentation=TableDocumentationStrategy.BEFORE_COMMIT,
            transaction=transaction,
            hooks=hooks,
            statement=statement,
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
            "facts": self.facts.to_dict() if self.facts is not None else None,
            "operations": [item.to_dict() for item in self.operations],
            "provenance": [item.to_dict() for item in self.provenance],
        }


def resolve_table_materialization_operations(
    plan: TableLifecyclePlan,
    facts: TableMaterializationFacts,
) -> Tuple[MaterializationOperation, ...]:
    """Compile lifecycle policies and live facts into an ordered program."""

    auto_begin = plan.statement == MaterializationStatementStrategy.AUTO_BEGIN
    target = MaterializationRelationRole.TARGET
    existing = MaterializationRelationRole.EXISTING
    intermediate = MaterializationRelationRole.INTERMEDIATE
    backup = MaterializationRelationRole.BACKUP

    operations = []
    if plan.setup_macro is not None:
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.INVOKE_CALLBACK,
                name=plan.setup_macro,
            )
        )

    if plan.replacement == TableReplacementStrategy.STAGE_AND_SWAP:
        operations.extend(
            (
                MaterializationOperation(
                    kind=MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
                    relation=intermediate,
                ),
                MaterializationOperation(
                    kind=MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
                    relation=backup,
                ),
            )
        )
        if plan.hooks == MaterializationHookStrategy.SPLIT:
            operations.append(
                MaterializationOperation(
                    kind=MaterializationOperationKind.RUN_HOOKS,
                    name="pre",
                    inside_transaction=False,
                )
            )
        operations.extend(
            (
                MaterializationOperation(
                    kind=MaterializationOperationKind.RUN_HOOKS,
                    name="pre",
                    inside_transaction=True,
                ),
                MaterializationOperation(
                    kind=MaterializationOperationKind.CREATE_FROM_QUERY,
                    relation=intermediate,
                    temporary=False,
                    auto_begin=auto_begin,
                ),
            )
        )
        if plan.indexes == TableIndexStrategy.BEFORE_SWAP:
            operations.append(
                MaterializationOperation(
                    kind=MaterializationOperationKind.CREATE_INDEXES,
                    relation=intermediate,
                )
            )
        if facts.existing is not None:
            if plan.existing_indexes == ExistingIndexStrategy.DROP_BEFORE_SWAP:
                operations.append(
                    MaterializationOperation(
                        kind=MaterializationOperationKind.INVOKE_CALLBACK,
                        relation=existing,
                        name="drop_indexes_on_relation",
                    )
                )
            if facts.existing.can_be_renamed:
                operations.append(
                    MaterializationOperation(
                        kind=MaterializationOperationKind.RENAME_RELATION,
                        relation=existing,
                        destination=backup,
                    )
                )
            else:
                operations.append(
                    MaterializationOperation(
                        kind=MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
                        relation=existing,
                    )
                )
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.RENAME_RELATION,
                relation=intermediate,
                destination=target,
            )
        )
        if plan.indexes == TableIndexStrategy.AFTER_SWAP:
            operations.append(
                MaterializationOperation(
                    kind=MaterializationOperationKind.CREATE_INDEXES,
                    relation=target,
                )
            )
    else:
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.RUN_HOOKS,
                name="pre",
                inside_transaction=True,
            )
        )
        if facts.existing is not None and facts.existing.requires_drop_before_replace:
            operations.append(
                MaterializationOperation(
                    kind=MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
                    relation=existing,
                )
            )
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.CREATE_FROM_QUERY,
                relation=target,
                temporary=False,
                auto_begin=auto_begin,
            )
        )

    operations.extend(
        (
            MaterializationOperation(
                kind=MaterializationOperationKind.RUN_HOOKS,
                name="post",
                inside_transaction=True,
            ),
            MaterializationOperation(
                kind=MaterializationOperationKind.APPLY_GRANTS,
                relation=target,
            ),
        )
    )
    if plan.documentation == TableDocumentationStrategy.BEFORE_COMMIT:
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.PERSIST_DOCUMENTATION,
                relation=target,
            )
        )
    if plan.transaction == MaterializationTransactionStrategy.EXPLICIT_COMMIT:
        operations.append(MaterializationOperation(kind=MaterializationOperationKind.COMMIT))
    if plan.documentation == TableDocumentationStrategy.AFTER_COMMIT:
        operations.extend(
            (
                MaterializationOperation(
                    kind=MaterializationOperationKind.PERSIST_DOCUMENTATION,
                    relation=target,
                ),
                MaterializationOperation(kind=MaterializationOperationKind.COMMIT),
            )
        )
    if plan.replacement == TableReplacementStrategy.STAGE_AND_SWAP:
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
                relation=backup,
            )
        )
        if plan.hooks == MaterializationHookStrategy.SPLIT:
            operations.append(
                MaterializationOperation(
                    kind=MaterializationOperationKind.RUN_HOOKS,
                    name="post",
                    inside_transaction=False,
                )
            )
    if plan.teardown_macro is not None:
        operations.append(
            MaterializationOperation(
                kind=MaterializationOperationKind.INVOKE_CALLBACK,
                name=plan.teardown_macro,
            )
        )
    return tuple(operations)
