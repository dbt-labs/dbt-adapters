"""Runtime patches for pyspark Spark Connect bugs (pyspark imported lazily)."""

from __future__ import annotations

import threading

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
