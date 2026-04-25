"""Runtime patches for pyspark Spark Connect bugs that affect parallel execution.

pyspark is imported lazily so this module stays importable without the optional
Spark Connect dependency installed.
"""

from __future__ import annotations

import threading
from typing import Any

_patches_applied = False
_patch_lock = threading.Lock()


def apply_pyspark_workarounds() -> None:
    """Idempotently apply runtime patches to pyspark Spark Connect.

    Call once before opening any Spark Connect session.  Safe to call from
    any thread; subsequent calls are no-ops.
    """
    global _patches_applied
    if _patches_applied:
        return
    with _patch_lock:
        if _patches_applied:
            return
        _neutralize_release_thread_pool_shutdown()
        _patches_applied = True


def _neutralize_release_thread_pool_shutdown() -> None:
    """Make ExecutePlanResponseReattachableIterator.shutdown a no-op.

    pyspark 3.5.x shares ``_release_thread_pool`` as a class-level
    ``multiprocessing.pool.ThreadPool`` across every Spark Connect session in
    the process.  ``_release_until`` / ``_release_all`` check
    ``pool is not None`` *without* the class lock, then call
    ``apply_async`` on it.  ``SparkConnectClient.close()`` (invoked by
    ``SparkSession.stop()``) calls
    ``ExecutePlanResponseReattachableIterator.shutdown()``, which closes the
    pool and sets it to ``None``.  Concurrent sessions race: once one session
    stops, an in-flight iterator in another thread hits
    ``ValueError("Pool not running")`` from ``ThreadPool._check_running()``.

    Apache Spark fixed this in SPARK-55406 by tracking iterators with a
    ``WeakSet`` and shutting the pool down only when no iterators remain
    alive.  The fix landed in master / 4.x and is not backported to
    branch-3.5; Athena's Spark Connect endpoint is Spark 3.5 server-side,
    so we cannot upgrade pyspark past 3.5 either.  Patching ``shutdown`` to
    a no-op locally achieves the same invariant the SPARK-55406 fix relies
    on: the pool is never shut down while iterators are still alive.

    We deliberately do not replicate the SPARK-55406 ``shutdown_threadpool_if_idle``
    path that closes the pool when no iterators remain.  That branch only
    fires during a "quiet moment" with zero live iterators, which is
    vanishingly rare during a parallel ``dbt run`` and converges with our
    behavior at process exit anyway.  The savings (a handful of idle
    daemon threads for the duration of a quiet window) are not worth the
    additional patching surface.

    Leaking the pool for the lifetime of the process is harmless: it holds a
    small number of daemon worker threads that issue fire-and-forget
    ``ReleaseExecute`` RPCs and is reclaimed when the process exits.

    References:
        Buggy code (pyspark 3.5.x):
            https://github.com/apache/spark/blob/branch-3.5/python/pyspark/sql/connect/client/reattach.py#L57-L80
        Upstream fix (SPARK-55406, commit d54498861119):
            https://github.com/apache/spark/commit/d54498861119989b041399b68545972ce851e133
            https://issues.apache.org/jira/browse/SPARK-55406
    """
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    def _noop_shutdown(cls: Any) -> None:  # noqa: ARG001
        return None

    ExecutePlanResponseReattachableIterator.shutdown = classmethod(_noop_shutdown)
