from dbt_common.exceptions import CompilationError, DbtRuntimeError
from typing import Optional
from mypy_boto3_athena.type_defs import AthenaErrorTypeDef


class SnapshotMigrationRequired(CompilationError):
    """Hive snapshot requires a manual operation due to backward incompatible changes."""


class S3LocationException(DbtRuntimeError):
    pass


class AthenaError(DbtRuntimeError):
    pass


class AthenaQueryCancelledError(AthenaError):
    pass


class AthenaQueryFailedError(AthenaError):
    CATEGORY_SYSTEM = 1
    CATEGORY_USER = 2
    CATEGORY_OTHER = 3

    TYPE_ICEBERG_ERROR = 233

    error_category: Optional[int]
    error_type: Optional[int]
    retryable: Optional[bool]

    def __init__(
        self,
        athena_error: AthenaErrorTypeDef,
        state_change_reason: Optional[str] = None,
    ) -> None:
        # Athena does not always populate AthenaError.ErrorMessage; for some failures (e.g.
        # TOO_MANY_OPEN_PARTITIONS, ICEBERG_COMMIT_ERROR) the detail is only in StateChangeReason.
        # Fall back to it so the message is never empty and string-based error detection keeps working.
        super().__init__(athena_error.get("ErrorMessage") or state_change_reason)
        self.error_category = athena_error.get("ErrorCategory", None)
        self.error_type = athena_error.get("ErrorType", None)
        self.retryable = athena_error.get("Retryable", None)


# Legacy version of AthenaQueryCancelledError
class CancelledQueryException(AthenaQueryCancelledError):
    pass
