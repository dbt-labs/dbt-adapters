"""Runtime patches for pyspark Spark Connect bugs (pyspark imported lazily)."""

from __future__ import annotations

import threading
import time
from typing import Any, List, Optional

from dbt.adapters.events.logging import AdapterLogger

LOGGER = AdapterLogger(__name__)

_patches_applied = False
_patch_lock = threading.Lock()


def apply_pyspark_workarounds() -> None:
    global _patches_applied
    if _patches_applied:
        return
    with _patch_lock:
        if _patches_applied:
            return
        _neutralize_release_thread_pool_shutdown()
        _silence_release_all_warning()
        # Athena AuthToken refresh across pyspark's reattach cycle.
        # Mirrors SPARK-57425 (apache/spark#56497) until upstream lands:
        #   1. Stash the ChannelBuilder on the gRPC stub.
        #   2. Refresh metadata via the builder before each RPC (ReattachExecute,
        #      ReleaseExecute, retry ExecutePlan).
        _stash_channel_builder_on_stub()
        _refresh_reattach_iterator_metadata()
        _track_retry_blocks()
        _retry_permission_denied_in_spark_client()
        _patches_applied = True


def _neutralize_release_thread_pool_shutdown() -> None:
    """Make ``ExecutePlanResponseReattachableIterator.shutdown`` a no-op.

    pyspark 3.5 races on a class-level ThreadPool shared across sessions:
    one session's ``stop()`` shuts the pool down while another session's
    in-flight iterator hits ``ValueError("Pool not running")``.  Apache
    Spark fixed this in SPARK-55406 (master / 4.x only; not backported to
    branch-3.5).  Athena's Spark Connect endpoint is 3.5 server-side, so
    we cannot upgrade pyspark either; we patch ``shutdown`` locally and
    let the pool leak — the daemon threads are reclaimed at process exit.

    https://issues.apache.org/jira/browse/SPARK-55406
    """
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    def _noop_shutdown(cls: type) -> None:  # noqa: ARG001
        return None

    ExecutePlanResponseReattachableIterator.shutdown = classmethod(_noop_shutdown)


def _silence_release_all_warning() -> None:
    """Silence pyspark's ``_release_all`` ReleaseExecute warning.

    pyspark fires ``warnings.warn(...)`` from a fire-and-forget RPC its own
    docstring says the server is "equipped to deal with abandoned executions"
    for.  dbt-athena ends each python model with ``spark.stop()``, which
    closes the channel before the async release thread runs, so this warning
    fires dozens of times per build with no diagnostic value.
    """
    import warnings

    warnings.filterwarnings(
        "ignore",
        message=r"ReleaseExecute failed with exception:.*",
    )


_PERMISSION_DENIED_RETRY_WINDOW_SECONDS = 600

_RETRY_BLOCK_THREAD_LOCAL = threading.local()


class _RetryBlock:
    __slots__ = ("permission_denied_since",)

    def __init__(self) -> None:
        self.permission_denied_since: Optional[float] = None


def _stash_channel_builder_on_stub() -> None:
    """Cache the ChannelBuilder on the gRPC stub so the reattach iterator can find it."""
    from pyspark.sql.connect.client.core import SparkConnectClient

    original_init = SparkConnectClient.__init__

    def _patched_init(self: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        builder = getattr(self, "_builder", None)
        stub = getattr(self, "_stub", None)
        if (
            stub is not None
            and builder is not None
            and callable(getattr(builder, "metadata", None))
        ):
            stub._dbt_athena_builder = builder
            LOGGER.debug(
                "Stashed AthenaChannelBuilder on Spark Connect stub for metadata refresh."
            )

    SparkConnectClient.__init__ = _patched_init


def _refresh_reattach_iterator_metadata() -> None:
    """Refresh metadata before every RPC so the AuthToken can rotate mid-stream.

    pyspark captures ``metadata`` once at ``__init__`` and reuses the same
    list forever, which keeps Athena's 30-min ``x-aws-proxy-auth`` token
    pinned to its initial value. Upstream alignment follows SPARK-57425
    (apache/spark#56497): refresh before initial ExecutePlan retry / reattach
    / ReleaseExecute.
    """
    import grpc
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
        RetryException,
    )

    original_init = ExecutePlanResponseReattachableIterator.__init__
    original_call_iter = ExecutePlanResponseReattachableIterator._call_iter
    original_release_until = ExecutePlanResponseReattachableIterator._release_until
    original_release_all = ExecutePlanResponseReattachableIterator._release_all

    def _refresh(self: Any) -> None:
        builder = getattr(self, "_dbt_athena_channel_builder", None)
        if builder is None:
            return
        old_token = getattr(builder, "_auth_token", None)
        try:
            self._metadata = builder.metadata()
        except Exception as e:  # noqa: BLE001 - refresh is best-effort
            LOGGER.warning(f"Metadata refresh failed: {e}")
            return
        new_token = getattr(builder, "_auth_token", None)
        if new_token is not None and new_token != old_token:
            LOGGER.debug("Metadata refreshed: AuthToken rotated.")

    def _patched_init(self: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        self._dbt_athena_channel_builder = getattr(self._stub, "_dbt_athena_builder", None)

    def _patched_call_iter(self: Any, iter_fun: Any) -> Any:
        if self._iterator is None:
            _refresh(self)
        try:
            return original_call_iter(self, iter_fun)
        except grpc.RpcError as e:
            # A request rejected by the Athena proxy never reaches the server, so a
            # ReattachExecute for a client whose first ExecutePlan was rejected finds no
            # server-side session.
            if (
                self._last_returned_response_id is not None
                or "INVALID_HANDLE.SESSION_NOT_FOUND" not in str(e)
            ):
                raise
            _refresh(self)
            self._iterator = iter(
                self._stub.ExecutePlan(self._initial_request, metadata=self._metadata)
            )
            raise RetryException() from e

    def _patched_release_until(self: Any, until_response_id: str) -> Any:
        _refresh(self)
        return original_release_until(self, until_response_id)

    def _patched_release_all(self: Any) -> Any:
        _refresh(self)
        return original_release_all(self)

    ExecutePlanResponseReattachableIterator.__init__ = _patched_init
    ExecutePlanResponseReattachableIterator._call_iter = _patched_call_iter
    ExecutePlanResponseReattachableIterator._release_until = _patched_release_until
    ExecutePlanResponseReattachableIterator._release_all = _patched_release_all


def _track_retry_blocks() -> None:
    from pyspark.sql.connect.client.core import Retrying

    original_iter = Retrying.__iter__

    def _patched_iter(self: Any) -> Any:
        stack: Optional[List[_RetryBlock]] = getattr(_RETRY_BLOCK_THREAD_LOCAL, "stack", None)
        if stack is None:
            stack = []
            _RETRY_BLOCK_THREAD_LOCAL.stack = stack
        block = _RetryBlock()
        stack.append(block)
        try:
            yield from original_iter(self)
        finally:
            for index in range(len(stack) - 1, -1, -1):
                if stack[index] is block:
                    del stack[index]
                    break

    Retrying.__iter__ = _patched_iter


def _retry_permission_denied_in_spark_client() -> None:
    """Retry PERMISSION_DENIED with pyspark's backoff for a bounded window.

    Athena's Spark Connect proxy answers PERMISSION_DENIED both when the
    AuthToken expires and when request volume exceeds a limit shared across
    sessions. The latter rejects every session, including newly started
    ones, for minutes, so an immediate single retry cannot recover.
    """
    import grpc
    from pyspark.sql.connect.client.core import SparkConnectClient

    original = SparkConnectClient.retry_exception.__func__

    def _patched(cls: Any, e: BaseException) -> bool:
        if original(cls, e):
            return True
        if not (isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.PERMISSION_DENIED):
            return False
        stack = getattr(_RETRY_BLOCK_THREAD_LOCAL, "stack", None)
        if not stack:
            LOGGER.warning("PERMISSION_DENIED outside a pyspark retry loop; propagating.")
            return False
        block = stack[-1]
        now = time.monotonic()
        if block.permission_denied_since is None:
            block.permission_denied_since = now
            LOGGER.warning(
                "PERMISSION_DENIED from Athena Spark Connect; retrying with backoff "
                f"for up to {_PERMISSION_DENIED_RETRY_WINDOW_SECONDS}s."
            )
            return True
        if now - block.permission_denied_since <= _PERMISSION_DENIED_RETRY_WINDOW_SECONDS:
            return True
        LOGGER.warning(
            f"PERMISSION_DENIED persisted for {_PERMISSION_DENIED_RETRY_WINDOW_SECONDS}s; propagating."
        )
        return False

    SparkConnectClient.retry_exception = classmethod(_patched)
