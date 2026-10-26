"""Connection management for lake compute.

* One shared ``AdbcDatabase`` per target, one ``AdbcConnection`` per thread. Auth runs once
  per database, and the driver allows concurrent ``NewConnection``.
* Connections are built by hand with ``autocommit=True``. ``Connection.adbc_clone()`` would
  hardcode ``autocommit=False``, and the service has no transactions, so dbt's transaction
  state machine is reduced to flag-flips.
* ``get_response`` is pure: query metadata is read once and stashed on the cursor for the
  caller and the structured query-status event.
"""

import atexit
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast, Dict, Iterable, Iterator, List, Optional, Tuple, TYPE_CHECKING

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.utils import cast_to_str

from dbt.adapters.lakecompute.credentials import LakeComputeCredentials
from dbt.adapters.lakecompute.driver import ResolvedDriver, resolve_driver
from dbt.adapters.lakecompute.type_mapping import data_type_code_to_name, normalize_value

if TYPE_CHECKING:
    import agate

logger = AdapterLogger("LakeCompute")


OPTION_LAST_QUERY_ID = "adbc.dbt.last_query_id"
OPTION_LAST_WARNINGS = "adbc.dbt.last_warnings"
OPTION_PROJECT = "adbc.dbt.project"
OPTION_MODEL = "adbc.dbt.model"
OPTION_RUN_ID = "adbc.dbt.run_id"

_ATTR_QUERY_ID = "_lakecompute_query_id"
_ATTR_WARNINGS = "_lakecompute_warnings"

_ATTR_ACTIVE_STATEMENTS = "_lakecompute_active_statements"


@dataclass
class LakeComputeAdapterResponse(AdapterResponse):
    warning: Optional[str] = None


@dataclass(frozen=True)
class _DatabaseKey:
    """Identity of a shared database; includes the db kwargs so two targets never share one."""

    path: str
    entrypoint: str
    db_kwargs: Tuple[Tuple[str, str], ...]

    @classmethod
    def from_resolved(cls, resolved: ResolvedDriver) -> "_DatabaseKey":
        return cls(
            path=resolved.path,
            entrypoint=resolved.entrypoint,
            db_kwargs=tuple(sorted(resolved.db_kwargs.items())),
        )


_REGISTRY: Dict[_DatabaseKey, Any] = {}
_REGISTRY_LOCK = threading.Lock()


def _close_all_databases() -> None:
    """Release every shared database at interpreter exit.

    Not wired into ``cleanup_all()``: dbt can call that mid-process, and dbt-tests-adapter
    expects the databases to stay live.
    """
    with _REGISTRY_LOCK:
        shared_databases = list(_REGISTRY.values())
        _REGISTRY.clear()
    for shared in shared_databases:
        try:
            shared.close()
        except Exception as exc:  # pragma: no cover - best effort at shutdown
            logger.debug(f"Error closing shared lakecompute database: {exc}")


atexit.register(_close_all_databases)


def _new_database(resolved: ResolvedDriver) -> Any:
    """Construct one ``AdbcDatabase`` and wrap it in a refcounted ``_SharedDatabase``."""
    from adbc_driver_manager import _lib, dbapi

    database = _lib.AdbcDatabase(
        driver=resolved.path,
        entrypoint=resolved.entrypoint,
        **resolved.db_kwargs,
    )
    return dbapi._SharedDatabase(database)


def _get_or_create_shared_database(resolved: ResolvedDriver) -> Any:
    key = _DatabaseKey.from_resolved(resolved)

    shared = _REGISTRY.get(key)
    if shared is not None:
        return shared

    with _REGISTRY_LOCK:
        # re-check: another thread may have published while we waited
        shared = _REGISTRY.get(key)
        if shared is not None:
            return shared

        shared = _new_database(resolved)
        _REGISTRY[key] = shared
        return shared


def _new_thread_connection(shared: Any, resolved: ResolvedDriver) -> Any:
    """Build one autocommit ``dbapi.Connection`` over ``shared``.

    ``Connection.__init__`` clones the ``_SharedDatabase`` itself, so do not pre-clone.
    """
    from adbc_driver_manager import _lib, dbapi

    conn_kwargs: Dict[str, str] = {}
    adbc_connection = _lib.AdbcConnection(shared._db, **conn_kwargs)
    return dbapi.Connection(
        shared,
        adbc_connection,
        conn_kwargs,
        autocommit=True,
    )


_CANCEL_LOCK = threading.Lock()


@contextmanager
def _cancellable(connection: Connection, cursor: Any) -> Iterator[None]:
    """Publish the in-flight statement on ``connection`` for the duration of the call.

    ``cancel()`` needs the ``AdbcStatement``, because the driver only implements
    statement-level cancel.
    """
    statement = cursor.adbc_statement

    with _CANCEL_LOCK:
        active: Optional[List[Any]] = getattr(connection, _ATTR_ACTIVE_STATEMENTS, None)
        if active is None:
            active = []
            setattr(connection, _ATTR_ACTIVE_STATEMENTS, active)
        active.append(statement)

    try:
        yield
    finally:
        with _CANCEL_LOCK:
            # identity, not equality: AdbcStatement does not define __eq__
            for index, candidate in enumerate(active):
                if candidate is statement:
                    del active[index]
                    break


class LakeComputeConnectionManager(SQLConnectionManager):
    TYPE = "lakecompute"

    def set_query_header(self, query_header_context: Dict[str, Any]) -> None:
        # off unless the profile opts in; `_add_query_comment` is a no-op when this is None
        credentials = cast(LakeComputeCredentials, self.profile.credentials)
        if credentials.query_comment_enabled:
            super().set_query_header(query_header_context)
        else:
            self.query_header = None

    # The connection is autocommit, so these only flip dbt's bookkeeping flags. Overriding
    # here rather than no-oping add_begin_query/add_commit_query also avoids logging
    # SQLCommit events for commits that never happen.

    def begin(self) -> Connection:  # type: ignore[override]
        connection = self.get_thread_connection()
        connection.transaction_open = True
        return connection

    def commit(self) -> Connection:  # type: ignore[override]
        connection = self.get_thread_connection()
        connection.transaction_open = False
        return connection

    def clear_transaction(self) -> None:
        connection = self.get_if_exists()
        if connection is not None:
            connection.transaction_open = False

    def rollback_if_open(self) -> None:
        connection = self.get_if_exists()
        if connection is not None:
            connection.transaction_open = False

    def add_begin_query(self) -> None:  # type: ignore[override]
        return None

    def add_commit_query(self) -> None:  # type: ignore[override]
        return None

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: LakeComputeCredentials = connection.credentials
        resolved = resolve_driver(credentials)

        def connect() -> Any:
            shared = _get_or_create_shared_database(resolved)
            return _new_thread_connection(shared, resolved)

        from adbc_driver_manager import OperationalError

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=(OperationalError,),
        )

    def cancel(self, connection: Connection) -> None:
        """Cancel the statements currently in flight on ``connection``.

        Statement-level, because the driver returns ``NOT_IMPLEMENTED`` for
        ``AdbcConnectionCancel``. ``AdbcStatementCancel`` is safe to call from another thread.
        A cancelled query surfaces on the blocked thread as ADBC ``INTERNAL`` with "... ended
        with status Canceled", not as ``CANCELLED``.

        The cancels run under ``_CANCEL_LOCK``: ``AdbcStatement.cancel`` takes no lock of its
        own, so without it the owning thread could leave ``_cancellable`` and release the
        statement while the cancel is still inside the driver.
        """
        with _CANCEL_LOCK:
            statements = list(getattr(connection, _ATTR_ACTIVE_STATEMENTS, ()))

            if not statements:
                logger.debug(f"No in-flight lakecompute query to cancel on {connection.name}")
                return

            for statement in statements:
                try:
                    statement.cancel()
                except Exception as exc:
                    # don't mask the failure that prompted the cancel
                    logger.debug(f"Could not cancel lakecompute statement: {exc}")

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        try:
            yield
        except Exception as exc:
            # no transaction, so no rollback_if_open() here
            if isinstance(exc, DbtRuntimeError):
                raise

            status = _adbc_status_code(exc)
            credentials = cast(LakeComputeCredentials, self.profile.credentials)
            message = str(exc).strip()

            if status in ("UNAUTHENTICATED", "UNAUTHORIZED"):
                raise DbtDatabaseError(
                    f"lakecompute rejected the credentials for auth method "
                    f"'{credentials.method}': {message}"
                )

            if status in ("TIMEOUT", "CANCELLED"):
                raise DbtDatabaseError(
                    f"lakecompute query {status.lower()}: {message}. "
                    f"Consider raising `timeout_seconds` in the profile."
                )

            raise DbtDatabaseError(message)

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        """Run ``sql`` and, when asked, materialise its rows.

        Keep the statement cancellable and translate driver errors until fetching finishes.
        Each cursor owns an ``AdbcStatement`` and is closed before returning or raising.
        """
        from dbt_common.clients.agate_helper import empty_table

        sql = self._add_query_comment(sql)
        with self._query(sql, auto_begin, fetch=fetch, close_cursor=True) as (_, cursor):
            table = self.get_result_from_cursor(cursor, limit) if fetch else empty_table()
            return self.get_response(cursor), table

    def add_select_query(self, sql: str) -> Tuple[Connection, Any]:
        # Same as execute: `get_column_schema_from_query` reads `cursor.description`, which is
        # None on the no-fetch path. The cursor is handed back open, as the base contract says.
        return self.add_query(self._add_query_comment(sql), auto_begin=False, fetch=True)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[type, ...] = tuple(),
        retry_limit: int = 1,
        *,
        fetch: bool = False,
    ) -> Tuple[Connection, Any]:
        """Send ``sql`` and return the cursor.

        ``fetch`` is keyword-only so ``adapter.add_query(...)`` macro calls keep working. A
        ``;``-joined batch is sent verbatim as one request; the service runs every statement.
        """
        with self._query(sql, auto_begin, bindings, abridge_sql_log, fetch=fetch) as (
            connection,
            cursor,
        ):
            return connection, cursor

    @contextmanager
    def _query(
        self,
        sql: str,
        auto_begin: bool,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        *,
        fetch: bool = False,
        close_cursor: bool = False,
    ) -> Iterator[Tuple[Connection, Any]]:
        """Guard execution and optional result consumption as one operation.

        ``add_query`` transfers ownership of a successful cursor to its caller. ``execute``
        consumes it inside this context and requests cleanup on success too. Failures always
        close the cursor, after deregistration has waited for any concurrent cancel.
        """
        if bindings is not None:
            raise DbtRuntimeError(
                "lakecompute does not support parameter binding; render literals into the SQL "
                "instead."
            )

        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        fire_event(
            ConnectionUsed(
                conn_type=self.TYPE,
                conn_name=cast_to_str(connection.name),
                node_info=get_node_info(),
            )
        )

        with self.exception_handler(sql):
            log_sql = f"{sql[:512]}..." if abridge_sql_log else sql
            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name),
                    sql=log_sql,
                    node_info=get_node_info(),
                )
            )
            started = time.perf_counter()

            cursor = connection.handle.cursor()
            try:
                with _cancellable(connection, cursor):
                    self._apply_statement_options(cursor)
                    if fetch:
                        cursor.execute(sql)
                    else:
                        # execute_update keeps rowcount without producing a result set.
                        cursor.executemany(sql, None)

                    self._collect_metadata(cursor)
                    yield connection, cursor

                    response = self.get_response(cursor)
                    fire_event(
                        SQLQueryStatus(
                            status=str(response),
                            elapsed=time.perf_counter() - started,
                            node_info=get_node_info(),
                            query_id=response.query_id,
                        )
                    )
            except BaseException:
                # Preserve the query error even if releasing the statement also fails.
                try:
                    cursor.close()
                except Exception as exc:
                    logger.debug(f"Error closing failed lakecompute cursor: {exc}")
                raise
            else:
                if close_cursor:
                    cursor.close()

    def _apply_statement_options(self, cursor: Any) -> None:
        """Set ``adbc.dbt.*`` attribution options on the statement.

        Set after cursor construction so a rejected option cannot leave a half-built cursor,
        and a rejection only logs: attribution must never fail a run.
        """
        options = self._statement_options()
        if not options:
            return
        try:
            cursor.adbc_statement.set_options(**options)
        except Exception as exc:
            logger.debug(f"lakecompute driver rejected statement options {sorted(options)}: {exc}")

    def _statement_options(self) -> Dict[str, str]:
        from dbt_common.invocation import get_invocation_id

        options: Dict[str, str] = {}

        project_name = getattr(self.profile, "project_name", None)
        if project_name:
            options[OPTION_PROJECT] = str(project_name)

        # empty for on-run-start/on-run-end hooks and for `dbt debug`
        node_info = get_node_info() or {}
        node_name = node_info.get("node_name")
        if node_name:
            options[OPTION_MODEL] = str(node_name)

        invocation_id = get_invocation_id()
        if invocation_id:
            options[OPTION_RUN_ID] = str(invocation_id)

        return options

    @classmethod
    def _collect_metadata(cls, cursor: Any) -> None:
        # read now: a closed statement raises. Each read is best effort.
        setattr(cursor, _ATTR_QUERY_ID, _try_get_option(cursor, OPTION_LAST_QUERY_ID))
        setattr(cursor, _ATTR_WARNINGS, _try_get_option(cursor, OPTION_LAST_WARNINGS))

    @classmethod
    def get_response(cls, cursor: Any) -> LakeComputeAdapterResponse:
        """Build the response; must stay pure (see the module docstring)."""
        rows_affected = getattr(cursor, "rowcount", -1)
        if rows_affected is None or rows_affected < 0:
            rows_affected = None

        warning = getattr(cursor, _ATTR_WARNINGS, None)

        message = "SUCCESS" if rows_affected is None else f"SUCCESS {rows_affected}"
        if warning:
            message = f"{message} [{warning}]"

        return LakeComputeAdapterResponse(
            _message=message,
            code="SUCCESS",
            rows_affected=rows_affected,
            query_id=getattr(cursor, _ATTR_QUERY_ID, None),
            warning=warning,
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Any) -> str:
        # Lives on the connection manager because `get_column_schema_from_query` calls it as
        # `self.connections.data_type_code_to_name`. ADBC supplies a pyarrow DataType.
        return data_type_code_to_name(type_code)

    @classmethod
    def process_results(
        cls, column_names: Iterable[str], rows: Iterable[Any]
    ) -> Iterator[Dict[str, Any]]:
        for row in super().process_results(column_names, rows):
            yield {key: normalize_value(value) for key, value in row.items()}


def _adbc_status_code(exc: BaseException) -> str:
    """Best-effort name of the ``AdbcStatusCode`` behind an exception."""
    status = getattr(exc, "status_code", None)
    if status is None:
        return "UNKNOWN"
    name = getattr(status, "name", None)
    if name:
        return str(name).upper()
    return str(status).rsplit(".", 1)[-1].upper()


def _try_get_option(cursor: Any, option: str) -> Optional[str]:
    try:
        value = cursor.adbc_statement.get_option(option)
    except Exception:
        return None
    if value in (None, ""):
        return None
    return str(value)
