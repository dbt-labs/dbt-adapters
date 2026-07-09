"""Transient-error classification for Spark Connect retries."""

from __future__ import annotations

from typing import Iterator, Optional

TRANSIENT_SPARK_PATTERNS = [
    # Spark executor failed to obtain credentials from the provider chain.
    "Unable to load credentials",
    # Spark executor failed to resolve the AWS region (IMDS not yet ready).
    "Unable to load region",
    # gRPC connection pool was shut down; a new session creates a fresh one.
    "Pool not running",
    # Athena terminated the Spark session (idle timeout / DPU / quota).
    "Session not active",
    # Account/workgroup session quota exhausted; retry after others finish.
    "Maximum allowed sessions",
]

TRANSIENT_GRPC_STATUS_CODES = frozenset(
    # PERMISSION_DENIED reaches the job level only when pyspark_patches'
    # in-stream reattach has already given up, so a fresh session is the
    # only recovery path left.
    {"UNAVAILABLE", "DEADLINE_EXCEEDED", "ABORTED", "RESOURCE_EXHAUSTED", "PERMISSION_DENIED"}
)


def _iter_grpc_status_codes(e: BaseException) -> Iterator[str]:
    # pyspark wraps gRPC errors, so the code() callable can sit at any
    # depth in the cause chain.
    current: Optional[BaseException] = e
    while current is not None:
        code_fn = getattr(current, "code", None)
        if callable(code_fn):
            try:
                code = code_fn()
            except Exception:  # noqa: BLE001 - not a gRPC error
                code = None
            name = getattr(code, "name", None) if code is not None else None
            if name is not None:
                yield name
        current = current.__cause__ or current.__context__


def is_transient_spark_error(e: BaseException) -> bool:
    if any(name in TRANSIENT_GRPC_STATUS_CODES for name in _iter_grpc_status_codes(e)):
        return True
    error_str = f"{type(e).__name__}: {e}"
    return any(p in error_str for p in TRANSIENT_SPARK_PATTERNS)


def is_grpc_permission_denied(e: BaseException) -> bool:
    return any(name == "PERMISSION_DENIED" for name in _iter_grpc_status_codes(e))
