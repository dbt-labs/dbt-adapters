import abc
import time
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Type,
)

from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtInternalError, NotImplementedError
from dbt_common.utils import cast_to_str

from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.types import (
    ConnectionUsed,
    SQLCommit,
    SQLQuery,
    SQLQueryStatus,
    AdapterEventDebug,
)

if TYPE_CHECKING:
    import agate


class SQLConnectionManager(BaseConnectionManager):
    """The default connection manager with some common SQL methods implemented.

    Methods to implement:
        - exception_handler
        - cancel
        - get_response
        - open
    """

    @abc.abstractmethod
    def cancel(self, connection: Connection):
        """Cancel the given connection."""
        raise NotImplementedError("`cancel` is not implemented for this adapter!")

    def cancel_open(self) -> List[str]:
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for connection in self.thread_connections.values():
                if connection is this_connection:
                    continue

                # if the connection failed, the handle will be None so we have
                # nothing to cancel.
                if connection.handle is not None and connection.state == ConnectionState.OPEN:
                    self.cancel(connection)
                if connection.name is not None:
                    names.append(connection.name)
        return names

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[Type[Exception], ...] = tuple(),
        retry_limit: int = 1,
    ) -> Tuple[Connection, Any]:
        """
        Retry function encapsulated here to avoid commitment to some
        user-facing interface. Right now, Redshift commits to a 1 second
        retry timeout so this serves as a default.
        """

        def _execute_query_with_retry(
            cursor: Any,
            sql: str,
            bindings: Optional[Any],
            retryable_exceptions: Tuple[Type[Exception], ...],
            retry_limit: int,
            attempt: int,
        ):
            """
            A success sees the try exit cleanly and avoid any recursive
            retries. Failure begins a sleep and retry routine.
            """
            try:
                cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                # Cease retries and fail when limit is hit.
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        message=f"Got a retryable error {type(e)}. {retry_limit - attempt} retries left. Retrying in 1 second.\nError:\n{e}"
                    )
                )
                time.sleep(1)

                return _execute_query_with_retry(
                    cursor=cursor,
                    sql=sql,
                    bindings=bindings,
                    retryable_exceptions=retryable_exceptions,
                    retry_limit=retry_limit,
                    attempt=attempt + 1,
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
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name),
                    sql=log_sql,
                    node_info=get_node_info(),
                )
            )

            pre = time.perf_counter()

            cursor = connection.handle.cursor()
            _execute_query_with_retry(
                cursor=cursor,
                sql=sql,
                bindings=bindings,
                retryable_exceptions=retryable_exceptions,
                retry_limit=retry_limit,
                attempt=1,
            )

            result = self.get_response(cursor)

            fire_event(
                SQLQueryStatus(
                    status=str(result),
                    elapsed=time.perf_counter() - pre,
                    node_info=get_node_info(),
                    query_id=result.query_id,
                )
            )

            return connection, cursor

    @classmethod
    @abc.abstractmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        """Get the status of the cursor."""
        raise NotImplementedError("`get_response` is not implemented for this adapter!")

    @classmethod
    def process_results(
        cls, column_names: Iterable[str], rows: Iterable[Any]
    ) -> Iterator[Dict[str, Any]]:
        unique_col_names = dict()  # type: ignore[var-annotated]
        for idx in range(len(column_names)):  # type: ignore[arg-type]
            col_name = column_names[idx]  # type: ignore[index]
            if col_name in unique_col_names:
                unique_col_names[col_name] += 1
                column_names[idx] = f"{col_name}_{unique_col_names[col_name]}"  # type: ignore[index] # noqa
            else:
                unique_col_names[column_names[idx]] = 1  # type: ignore[index]

        for row in rows:
            yield dict(zip(column_names, row))

    @classmethod
    def get_result_from_cursor(cls, cursor: Any, limit: Optional[int]) -> "agate.Table":
        from dbt_common.clients.agate_helper import table_from_data_flat

        data: Iterable[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            if limit:
                rows = cursor.fetchmany(limit)
            else:
                rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return table_from_data_flat(data, column_names)

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        from dbt_common.clients.agate_helper import empty_table

        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        return response, table

    def add_begin_query(self):
        return self.add_query("BEGIN", auto_begin=False)

    def add_commit_query(self):
        return self.add_query("COMMIT", auto_begin=False)

    def add_select_query(self, sql: str) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        return self.add_query(sql, auto_begin=False)

    def begin(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is True:
            raise DbtInternalError(
                'Tried to begin a new transaction on connection "{}", but '
                "it already had one open!".format(connection.name)
            )

        self.add_begin_query()

        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is False:
            raise DbtInternalError(
                'Tried to commit transaction on connection "{}", but '
                "it does not have one open!".format(connection.name)
            )

        fire_event(SQLCommit(conn_name=connection.name, node_info=get_node_info()))
        self.add_commit_query()

        connection.transaction_open = False

        return connection
