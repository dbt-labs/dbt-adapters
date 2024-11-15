from dbt.adapters.exceptions.alias import AliasError, DuplicateAliasError
from dbt.adapters.exceptions.cache import (
    CacheInconsistencyError,
    DependentLinkNotCachedError,
    NewNameAlreadyInCacheError,
    NoneRelationFoundError,
    ReferencedLinkNotCachedError,
    TruncatedModelNameCausedCollisionError,
)
from dbt.adapters.exceptions.compilation import (
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
from dbt.adapters.exceptions.connection import (
    FailedToConnectError,
    InvalidConnectionError,
)
from dbt.adapters.exceptions.database import (
    CrossDbReferenceProhibitedError,
    IndexConfigError,
    IndexConfigNotDictError,
    UnexpectedDbReferenceError,
)
