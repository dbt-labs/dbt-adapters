from typing import Any

from dbt_common.exceptions import CompilationError, DbtRuntimeError


class SnapshotMigrationRequired(CompilationError):
    """Hive snapshot requires a manual operation due to backward incompatible changes."""


class S3LocationException(DbtRuntimeError):
    pass


class S3DeleteRetriableException(DbtRuntimeError):
    """Raised when S3 object deletion fails due to a retriable error (e.g., SlowDown, InternalError)."""

    def __init__(self, msg: str) -> None:
        """
        Initializes the S3DeleteRetriableException with a message.

        Args:
            msg (str): The message describing the error.
        """
        super().__init__(msg)
        self.msg = msg

    def __str__(self) -> Any:
        """
        Returns a string representation of the exception.

        Returns:
            str: The message of the exception.
        """
        return self.msg
