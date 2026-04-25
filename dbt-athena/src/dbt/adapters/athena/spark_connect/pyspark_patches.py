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

    def _noop_shutdown(cls: Any) -> None:  # noqa: ARG001
        return None

    ExecutePlanResponseReattachableIterator.shutdown = classmethod(_noop_shutdown)
