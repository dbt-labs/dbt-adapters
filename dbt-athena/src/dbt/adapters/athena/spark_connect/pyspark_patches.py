from __future__ import annotations

import hashlib
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, List, Optional

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
        # Athena AuthToken refresh across the reattach cycle; mirrors SPARK-57425
        # (apache/spark#56497).
        _stash_channel_builder_on_stub()
        _refresh_reattach_iterator_metadata()
        _refresh_artifact_manager_metadata()
        _track_retry_blocks()
        _retry_permission_denied_in_spark_client()
        _skip_artifacts_already_added()
        _patches_applied = True


def _neutralize_release_thread_pool_shutdown() -> None:
    """pyspark 3.5 shares a class-level ThreadPool across sessions; one session's ``stop()``
    closes it and other sessions' in-flight iterators fail with "Pool not running"
    (SPARK-55406, not backported to 3.5). The pool stays open until process exit.
    """
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    def _noop_shutdown(cls: type) -> None:  # noqa: ARG001
        return None

    ExecutePlanResponseReattachableIterator.shutdown = classmethod(_noop_shutdown)


def _silence_release_all_warning() -> None:
    """pyspark's ``_release_all`` is a fire-and-forget RPC that warns on any ReleaseExecute
    failure; the server copes with abandoned executions, so the warning carries no information.
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


@contextmanager
def client_retries_disabled() -> Iterator[None]:
    _RETRY_BLOCK_THREAD_LOCAL.disabled = True
    try:
        yield
    finally:
        _RETRY_BLOCK_THREAD_LOCAL.disabled = False


def _stash_channel_builder_on_stub() -> None:
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
            artifact_manager = getattr(self, "_artifact_manager", None)
            if artifact_manager is not None:
                artifact_manager._dbt_athena_builder = builder
            LOGGER.debug(
                "Stashed AthenaChannelBuilder on Spark Connect stub for metadata refresh."
            )

    SparkConnectClient.__init__ = _patched_init


def _refresh_metadata(obj: Any) -> None:
    builder = getattr(obj, "_dbt_athena_builder", None)
    if builder is None:
        return
    old_token = getattr(builder, "_auth_token", None)
    try:
        obj._metadata = builder.metadata()
    except Exception as e:  # noqa: BLE001 - refresh is best-effort
        LOGGER.warning(f"Metadata refresh failed: {e}")
        return
    new_token = getattr(builder, "_auth_token", None)
    if new_token is not None and new_token != old_token:
        LOGGER.debug("Metadata refreshed: AuthToken rotated.")


def _refresh_artifact_manager_metadata() -> None:
    from pyspark.sql.connect.client.artifact import ArtifactManager

    original_retrieve_responses = ArtifactManager._retrieve_responses
    original_is_cached_artifact = ArtifactManager.is_cached_artifact

    def _patched_retrieve_responses(self: Any, *args: Any, **kwargs: Any) -> Any:
        _refresh_metadata(self)
        return original_retrieve_responses(self, *args, **kwargs)

    def _patched_is_cached_artifact(self: Any, *args: Any, **kwargs: Any) -> Any:
        _refresh_metadata(self)
        return original_is_cached_artifact(self, *args, **kwargs)

    ArtifactManager._retrieve_responses = _patched_retrieve_responses
    ArtifactManager.is_cached_artifact = _patched_is_cached_artifact


def _refresh_reattach_iterator_metadata() -> None:
    """pyspark captures ``metadata`` once at ``__init__``, which pins Athena's 30-min
    ``x-aws-proxy-auth`` token to its initial value.
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

    _refresh = _refresh_metadata

    def _patched_init(self: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        self._dbt_athena_builder = getattr(self._stub, "_dbt_athena_builder", None)

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
        if getattr(_RETRY_BLOCK_THREAD_LOCAL, "disabled", False):
            return False
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


def _skip_artifacts_already_added() -> None:
    """Athena's Spark Connect server rejects an existing artifact name even with identical content."""
    from pyspark.sql.connect.client.artifact import ArtifactManager

    ArtifactManager.add_artifacts = _add_artifacts_once


def _artifact_digest(artifact: Any) -> str:
    # ``LocalFile.stream`` is a cached handle the upload reads later.
    digest = hashlib.sha256()
    with open(artifact.storage.path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _add_artifacts_once(self: Any, *path: str, pyfile: bool, archive: bool, file: bool) -> None:
    artifacts = [
        artifact
        for p in path
        for artifact in self._parse_artifacts(p, pyfile=pyfile, archive=archive, file=file)
    ]
    lock = self.__dict__.setdefault("_dbt_athena_added_artifacts_lock", threading.Lock())
    with lock:
        added = self.__dict__.setdefault("_dbt_athena_added_artifacts", {})
        pending = []
        for artifact in artifacts:
            digest = _artifact_digest(artifact)
            previous = added.get(artifact.path)
            if previous == digest:
                LOGGER.debug(f"Artifact {artifact.path} already added to this session; skipping.")
                continue
            if previous is not None:
                LOGGER.warning(
                    f"Artifact {artifact.path} was already added to this session with "
                    "different content; the server may reject the new upload."
                )
            pending.append((artifact, digest))
        if not pending:
            return
        self._request_add_artifacts(self._add_artifacts(artifact for artifact, _ in pending))
        for artifact, digest in pending:
            added[artifact.path] = digest
