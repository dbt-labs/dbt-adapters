from dbt.adapter.exceptions.alias import AliasError, DuplicateAliasError
from dbt.adapter.exceptions.cache import (
    CacheInconsistencyError,
    DependentLinkNotCachedError,
    NewNameAlreadyInCacheError,
    NoneRelationFoundError,
    ReferencedLinkNotCachedError,
    TruncatedModelNameCausedCollisionError,
)
from dbt.adapter.exceptions.compilation import (
    ApproximateMatchError,
    ColumnTypeMissingError,
    DuplicateMacroInPackageError,
    DuplicateMaterializationNameError,
    MacroNotFoundError,
    MaterializationNotAvailableError,
    MissingConfigError,
    MissingMaterializationError,
    MultipleDatabasesNotAllowedError,
    NullRelationCacheAttemptedError,
    NullRelationDropAttemptedError,
    QuoteConfigTypeError,
    RelationReturnedMultipleResultsError,
    RelationTypeNullError,
    RelationWrongTypeError,
    RenameToNoneAttemptedError,
    SnapshotTargetIncompleteError,
    SnapshotTargetNotSnapshotTableError,
    UnexpectedNonTimestampError,
)
from dbt.adapter.exceptions.connection import (
    FailedToConnectError,
    InvalidConnectionError,
)
from dbt.adapter.exceptions.database import (
    CrossDbReferenceProhibitedError,
    IndexConfigError,
    IndexConfigNotDictError,
    UnexpectedDbReferenceError,
)
