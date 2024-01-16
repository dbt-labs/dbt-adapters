import abc
import os
import sys
from time import sleep
import traceback
from multiprocessing.context import SpawnContext
from multiprocessing.synchronize import RLock
from threading import get_ident
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import agate
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtInternalError, NotImplementedError
from dbt_common.utils import cast_to_str

from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.contracts.connection import (
    AdapterRequiredConfig,
    AdapterResponse,
    Connection,
    ConnectionState,
    Identifier,
    LazyHandle,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import (
    ConnectionClosed,
    ConnectionClosedInCleanup,
    ConnectionLeftOpen,
    ConnectionLeftOpenInCleanup,
    ConnectionReused,
    NewConnection,
    Rollback,
    RollbackFailed,
)
from dbt.adapters.exceptions import FailedToConnectError, InvalidConnectionError


SleepTime = Union[int, float]  # As taken by time.sleep.
AdapterHandle = Any  # Adapter connection handle objects can be any class.


class BaseConnectionManager(metaclass=abc.ABCMeta):
    """Methods to implement:
        - exception_handler
        - cancel_open
        - open
        - begin
        - commit
        - clear_transaction
        - execute

    You must also set the 'TYPE' class attribute with a class-unique constant
    string.
    """

    TYPE: str = NotImplemented

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext) -> None:
        self.profile = profile
        self.thread_connections: Dict[Hashable, Connection] = {}
        self.lock: RLock = mp_context.RLock()
        self.query_header: Optional[MacroQueryStringSetter] = None

    def set_query_header(self, query_header_context: Dict[str, Any]) -> None:
        self.query_header = MacroQueryStringSetter(self.profile, query_header_context)

    @staticmethod
    def get_thread_identifier() -> Hashable:
        # note that get_ident() may be re-used, but we should never experience
        # that within a single process
        return os.getpid(), get_ident()

    def get_thread_connection(self) -> Connection:
        key = self.get_thread_identifier()
        with self.lock:
            if key not in self.thread_connections:
                raise InvalidConnectionError(key, list(self.thread_connections))
            return self.thread_connections[key]

    def set_thread_connection(self, conn: Connection) -> None:
        key = self.get_thread_identifier()
        if key in self.thread_connections:
            raise DbtInternalError("In set_thread_connection, existing connection exists for {}")
        self.thread_connections[key] = conn

    def get_if_exists(self) -> Optional[Connection]:
        key = self.get_thread_identifier()
        with self.lock:
            return self.thread_connections.get(key)

    def clear_thread_connection(self) -> None:
        key = self.get_thread_identifier()
        with self.lock:
            if key in self.thread_connections:
                del self.thread_connections[key]

    def clear_transaction(self) -> None:
        """Clear any existing transactions."""
        conn = self.get_thread_connection()
        if conn is not None:
            if conn.transaction_open:
                self._rollback(conn)
            self.begin()
            self.commit()

    def rollback_if_open(self) -> None:
        conn = self.get_if_exists()
        if conn is not None and conn.handle and conn.transaction_open:
            self._rollback(conn)

    @abc.abstractmethod
    def exception_handler(self, sql: str) -> ContextManager:
        """Create a context manager that handles exceptions caused by database
        interactions.

        :param str sql: The SQL string that the block inside the context
            manager is executing.
        :return: A context manager that handles exceptions raised by the
            underlying database.
        """
        raise NotImplementedError("`exception_handler` is not implemented for this adapter!")

    def set_connection_name(self, name: Optional[str] = None) -> Connection:
        """Called by 'acquire_connection' in BaseAdapter, which is called by
        'connection_named'.
        Creates a connection for this thread if one doesn't already
        exist, and will rename an existing connection."""

        conn_name: str = "master" if name is None else name

        # Get a connection for this thread
        conn = self.get_if_exists()

        if conn and conn.name == conn_name and conn.state == "open":
            # Found a connection and nothing to do, so just return it
            return conn

        if conn is None:
            # Create a new connection
            conn = Connection(
                type=Identifier(self.TYPE),
                name=conn_name,
                state=ConnectionState.INIT,
                transaction_open=False,
                handle=None,
                credentials=self.profile.credentials,
            )
            conn.handle = LazyHandle(self.open)
            # Add the connection to thread_connections for this thread
            self.set_thread_connection(conn)
            fire_event(
                NewConnection(conn_name=conn_name, conn_type=self.TYPE, node_info=get_node_info())
            )
        else:  # existing connection either wasn't open or didn't have the right name
            if conn.state != "open":
                conn.handle = LazyHandle(self.open)
            if conn.name != conn_name:
                orig_conn_name: str = conn.name or ""
                conn.name = conn_name
                fire_event(ConnectionReused(orig_conn_name=orig_conn_name, conn_name=conn_name))

        return conn

    @classmethod
    def retry_connection(
        cls,
        connection: Connection,
        connect: Callable[[], AdapterHandle],
        logger: AdapterLogger,
        retryable_exceptions: Iterable[Type[Exception]],
        retry_limit: int = 1,
        retry_timeout: Union[Callable[[int], SleepTime], SleepTime] = 1,
        _attempts: int = 0,
    ) -> Connection:
        """Given a Connection, set its handle by calling connect.

        The calls to connect will be retried up to retry_limit times to deal with transient
        connection errors. By default, one retry will be attempted if retryable_exceptions is set.

        :param Connection connection: An instance of a Connection that needs a handle to be set,
            usually when attempting to open it.
        :param connect: A callable that returns the appropiate connection handle for a
            given adapter. This callable will be retried retry_limit times if a subclass of any
            Exception in retryable_exceptions is raised by connect.
        :type connect: Callable[[], AdapterHandle]
        :param AdapterLogger logger: A logger to emit messages on retry attempts or errors. When
            handling expected errors, we call debug, and call warning on unexpected errors or when
            all retry attempts have been exhausted.
        :param retryable_exceptions: An iterable of exception classes that if raised by
            connect should trigger a retry.
        :type retryable_exceptions: Iterable[Type[Exception]]
        :param int retry_limit: How many times to retry the call to connect. If this limit
            is exceeded before a successful call, a FailedToConnectError will be raised.
            Must be non-negative.
        :param retry_timeout: Time to wait between attempts to connect. Can also take a
            Callable that takes the number of attempts so far, beginning at 0, and returns an int
            or float to be passed to time.sleep.
        :type retry_timeout: Union[Callable[[int], SleepTime], SleepTime] = 1
        :param int _attempts: Parameter used to keep track of the number of attempts in calling the
            connect function across recursive calls. Passed as an argument to retry_timeout if it
            is a Callable. This parameter should not be set by the initial caller.
        :raises dbt.adapters.exceptions.FailedToConnectError: Upon exhausting all retry attempts without
            successfully acquiring a handle.
        :return: The given connection with its appropriate state and handle attributes set
            depending on whether we successfully acquired a handle or not.
        """
        timeout = retry_timeout(_attempts) if callable(retry_timeout) else retry_timeout
        if timeout < 0:
            raise FailedToConnectError(
                "retry_timeout cannot be negative or return a negative time."
            )

        if retry_limit < 0 or retry_limit > sys.getrecursionlimit():
            # This guard is not perfect others may add to the recursion limit (e.g. built-ins).
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError("retry_limit cannot be negative")

        try:
            connection.handle = connect()
            connection.state = ConnectionState.OPEN
            return connection

        except tuple(retryable_exceptions) as e:
            if retry_limit <= 0:
                connection.handle = None
                connection.state = ConnectionState.FAIL
                raise FailedToConnectError(str(e))

            logger.debug(
                f"Got a retryable error when attempting to open a {cls.TYPE} connection.\n"
                f"{retry_limit} attempts remaining. Retrying in {timeout} seconds.\n"
                f"Error:\n{e}"
            )

            sleep(timeout)
            return cls.retry_connection(
                connection=connection,
                connect=connect,
                logger=logger,
                retry_limit=retry_limit - 1,
                retry_timeout=retry_timeout,
                retryable_exceptions=retryable_exceptions,
                _attempts=_attempts + 1,
            )

        except Exception as e:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(e))

    @abc.abstractmethod
    def cancel_open(self) -> Optional[List[str]]:
        """Cancel all open connections on the adapter. (passable)"""
        raise NotImplementedError("`cancel_open` is not implemented for this adapter!")

    @classmethod
    @abc.abstractmethod
    def open(cls, connection: Connection) -> Connection:
        """Open the given connection on the adapter and return it.

        This may mutate the given connection (in particular, its state and its
        handle).

        This should be thread-safe, or hold the lock if necessary. The given
        connection should not be in either in_use or available.
        """
        raise NotImplementedError("`open` is not implemented for this adapter!")

    def release(self) -> None:
        with self.lock:
            conn = self.get_if_exists()
            if conn is None:
                return

        try:
            # always close the connection. close() calls _rollback() if there
            # is an open transaction
            self.close(conn)
        except Exception:
            # if rollback or close failed, remove our busted connection
            self.clear_thread_connection()
            raise

    def cleanup_all(self) -> None:
        with self.lock:
            for connection in self.thread_connections.values():
                if connection.state not in {"closed", "init"}:
                    fire_event(ConnectionLeftOpenInCleanup(conn_name=cast_to_str(connection.name)))
                else:
                    fire_event(ConnectionClosedInCleanup(conn_name=cast_to_str(connection.name)))
                self.close(connection)

            # garbage collect these connections
            self.thread_connections.clear()

    @abc.abstractmethod
    def begin(self) -> None:
        """Begin a transaction. (passable)"""
        raise NotImplementedError("`begin` is not implemented for this adapter!")

    @abc.abstractmethod
    def commit(self) -> None:
        """Commit a transaction. (passable)"""
        raise NotImplementedError("`commit` is not implemented for this adapter!")

    @classmethod
    def _rollback_handle(cls, connection: Connection) -> None:
        """Perform the actual rollback operation."""
        try:
            connection.handle.rollback()
        except Exception:
            fire_event(
                RollbackFailed(
                    conn_name=cast_to_str(connection.name),
                    exc_info=traceback.format_exc(),
                    node_info=get_node_info(),
                )
            )

    @classmethod
    def _close_handle(cls, connection: Connection) -> None:
        """Perform the actual close operation."""
        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, "close"):
            fire_event(
                ConnectionClosed(conn_name=cast_to_str(connection.name), node_info=get_node_info())
            )
            connection.handle.close()
        else:
            fire_event(
                ConnectionLeftOpen(
                    conn_name=cast_to_str(connection.name), node_info=get_node_info()
                )
            )

    @classmethod
    def _rollback(cls, connection: Connection) -> None:
        """Roll back the given connection."""
        if connection.transaction_open is False:
            raise DbtInternalError(
                f"Tried to rollback transaction on connection "
                f'"{connection.name}", but it does not have one open!'
            )

        fire_event(Rollback(conn_name=cast_to_str(connection.name), node_info=get_node_info()))
        cls._rollback_handle(connection)

        connection.transaction_open = False

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        if connection.transaction_open and connection.handle:
            fire_event(Rollback(conn_name=cast_to_str(connection.name), node_info=get_node_info()))
            cls._rollback_handle(connection)
        connection.transaction_open = False

        cls._close_handle(connection)
        connection.state = ConnectionState.CLOSED

        return connection

    def commit_if_has_connection(self) -> None:
        """If the named connection exists, commit the current transaction."""
        connection = self.get_if_exists()
        if connection:
            self.commit()

    def _add_query_comment(self, sql: str) -> str:
        if self.query_header is None:
            return sql
        return self.query_header.add(sql)

    @abc.abstractmethod
    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        """Execute the given SQL.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :param int limit: If set, limits the result set
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, agate.Table]
        """
        raise NotImplementedError("`execute` is not implemented for this adapter!")

    def add_select_query(self, sql: str) -> Tuple[Connection, Any]:
        """
        This was added here because base.impl.BaseAdapter.get_column_schema_from_query expects it to be here.
        That method wouldn't work unless the adapter used sql.impl.SQLAdapter, sql.connections.SQLConnectionManager
        or defined this method on <Adapter>ConnectionManager before passing it in to <Adapter>Adapter.

        See https://github.com/dbt-labs/dbt-core/issues/8396 for more information.
        """
        raise NotImplementedError("`add_select_query` is not implemented for this adapter!")

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        """Get the string representation of the data type from the type_code."""
        # https://peps.python.org/pep-0249/#type-objects
        raise NotImplementedError("`data_type_code_to_name` is not implemented for this adapter!")
