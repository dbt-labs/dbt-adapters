"""Runtime patches for pyspark Spark Connect bugs (pyspark imported lazily)."""

from __future__ import annotations

import threading
import weakref
from typing import Any

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
        #   3. One PERMISSION_DENIED retry per consecutive failure; budget
        #      replenishes after every successful response.
        _stash_channel_builder_on_stub()
        _refresh_reattach_iterator_metadata()
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


# weakref so a reused worker thread does not pin a stale iterator.
# Assumes one in-flight iterator per thread (pyspark consumes synchronously);
# concurrent iterators would need a stack here.
_CURRENT_ITERATOR_THREAD_LOCAL = threading.local()


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
    / ReleaseExecute, and replenish the PERMISSION_DENIED retry budget on
    every successful ``_call_iter`` so a query outlasting multiple TTLs can
    rotate the token more than once.
    """
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
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
        self._dbt_athena_pd_retried = False
        _CURRENT_ITERATOR_THREAD_LOCAL.iterator_ref = weakref.ref(self)

    def _patched_call_iter(self: Any, iter_fun: Any) -> Any:
        if self._iterator is None:
            _refresh(self)
        result = original_call_iter(self, iter_fun)
        self._dbt_athena_pd_retried = False
        return result

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


def _retry_permission_denied_in_spark_client() -> None:
    """Treat PERMISSION_DENIED as retryable so a 403 from token expiry can recover.

    pyspark's default ``retry_exception`` only retries UNAVAILABLE (and one
    ``INTERNAL`` cursor case), so a 403 propagates out before the reattach
    iterator can re-issue ``ReattachExecute``. We allow one retry per
    consecutive failure: ``_refresh_reattach_iterator_metadata`` resets the
    budget on every successful response (matching upstream SPARK-57425), so
    a query that survives one rotation can survive subsequent ones. Two
    PERMISSION_DENIED in a row without an intervening success propagate.
    """
    import grpc
    from pyspark.sql.connect.client.core import SparkConnectClient

    original = SparkConnectClient.retry_exception.__func__

    def _patched(cls: Any, e: BaseException) -> bool:
        if original(cls, e):
            return True
        if not (isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.PERMISSION_DENIED):
            return False
        iterator_ref = getattr(_CURRENT_ITERATOR_THREAD_LOCAL, "iterator_ref", None)
        iterator = iterator_ref() if iterator_ref is not None else None
        if iterator is None or getattr(iterator, "_dbt_athena_pd_retried", False):
            LOGGER.warning("PERMISSION_DENIED retry budget exhausted; propagating.")
            return False
        iterator._dbt_athena_pd_retried = True
        LOGGER.debug("PERMISSION_DENIED detected; allowing one reattach with refreshed metadata.")
        return True

    SparkConnectClient.retry_exception = classmethod(_patched)
