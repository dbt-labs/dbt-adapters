"""Transient-error classification for Spark Connect retries."""

from __future__ import annotations

from typing import Optional

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
    {"UNAVAILABLE", "DEADLINE_EXCEEDED", "ABORTED", "RESOURCE_EXHAUSTED"}
)


def is_transient_spark_error(e: BaseException) -> bool:
    # gRPC errors may be wrapped by pyspark; walk the chain.
    current: Optional[BaseException] = e
    while current is not None:
        code_fn = getattr(current, "code", None)
        if callable(code_fn):
            try:
                code = code_fn()
            except Exception:  # noqa: BLE001 - not a gRPC error
                code = None
            if code is not None and getattr(code, "name", None) in TRANSIENT_GRPC_STATUS_CODES:
                return True
        current = current.__cause__ or current.__context__

    error_str = f"{type(e).__name__}: {e}"
    return any(p in error_str for p in TRANSIENT_SPARK_PATTERNS)
