"""Transient-error classification for Spark Connect retries."""

from __future__ import annotations

from typing import Dict, FrozenSet, Iterator, List, Optional

from dbt.adapters.athena.constants import SPARK_CONNECT_RETRY_CATEGORIES

SESSION_ENDED, CAPACITY, EXECUTOR_ENVIRONMENT, CONNECTION = SPARK_CONNECT_RETRY_CATEGORIES

SESSION_ENDED_PATTERNS = [
    # Athena terminated the Spark session (idle timeout / DPU / quota).
    "Session not active",
    # pyspark raises this for an RPC on a closed gRPC channel.
    "NO_ACTIVE_SESSION",
    # GetSessionEndpoint for a session Athena already stopped.
    "Session endpoint URL for Session in STOPPED state",
]

TRANSIENT_SPARK_PATTERNS_BY_CATEGORY: Dict[str, List[str]] = {
    SESSION_ENDED: SESSION_ENDED_PATTERNS,
    CAPACITY: [
        # Account/workgroup session quota exhausted; retry after others finish.
        "Maximum allowed sessions",
    ],
    EXECUTOR_ENVIRONMENT: [
        # Spark executor failed to obtain credentials from the provider chain.
        "Unable to load credentials",
        # Spark executor failed to resolve the AWS region (IMDS not yet ready).
        "Unable to load region",
    ],
    CONNECTION: [
        # gRPC connection pool was shut down; a new session creates a fresh one.
        "Pool not running",
    ],
}

TRANSIENT_SPARK_PATTERNS = [
    p for patterns in TRANSIENT_SPARK_PATTERNS_BY_CATEGORY.values() for p in patterns
]

TRANSIENT_GRPC_STATUS_CODES_BY_CATEGORY: Dict[str, FrozenSet[str]] = {
    CAPACITY: frozenset({"RESOURCE_EXHAUSTED"}),
    # PERMISSION_DENIED reaches the job level only when pyspark_patches'
    # in-stream reattach has already given up, so a fresh session is the
    # only recovery path left.
    CONNECTION: frozenset({"UNAVAILABLE", "DEADLINE_EXCEEDED", "ABORTED", "PERMISSION_DENIED"}),
}

TRANSIENT_GRPC_STATUS_CODES = frozenset(
    code for codes in TRANSIENT_GRPC_STATUS_CODES_BY_CATEGORY.values() for code in codes
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


def classify_transient_spark_error(e: BaseException) -> Optional[str]:
    """Return the transient category of ``e``, or None if it is not transient."""
    error_str = f"{type(e).__name__}: {e}"
    for category, patterns in TRANSIENT_SPARK_PATTERNS_BY_CATEGORY.items():
        if any(p in error_str for p in patterns):
            return category
    codes = set(_iter_grpc_status_codes(e))
    for category, category_codes in TRANSIENT_GRPC_STATUS_CODES_BY_CATEGORY.items():
        if codes & category_codes:
            return category
    return None


def is_transient_spark_error(e: BaseException) -> bool:
    return classify_transient_spark_error(e) is not None


def is_session_ended_error(e: BaseException) -> bool:
    error_str = f"{type(e).__name__}: {e}"
    return any(p in error_str for p in SESSION_ENDED_PATTERNS)


def is_grpc_permission_denied(e: BaseException) -> bool:
    return any(name == "PERMISSION_DENIED" for name in _iter_grpc_status_codes(e))
