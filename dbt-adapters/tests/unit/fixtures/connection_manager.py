from contextlib import contextmanager
from typing import Generator, List, Optional, Tuple, Any

import agate

from dbt.adapters.base.connections import BaseConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState


class ConnectionManagerStub(BaseConnectionManager):
    """
    A stub for a connection manager that does not connect to a database
    """

    raised_exceptions: List[Exception]

    @contextmanager
    def exception_handler(self, sql: str) -> Generator[None, Any, None]:  # type: ignore
        # catch all exceptions and put them on this class for inspection in tests
        try:
            yield
        except Exception as exc:
            self.raised_exceptions.append(exc)
        finally:
            pass

    def cancel_open(self) -> Optional[List[str]]:
        names = []
        for connection in self.thread_connections.values():
            if connection.state == ConnectionState.OPEN:
                connection.state = ConnectionState.CLOSED  # type: ignore
                if name := connection.name:
                    names.append(name)
        return names

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        # there's no database, so just change the state
        connection.state = ConnectionState.OPEN  # type: ignore
        return connection

    def begin(self) -> None:
        # there's no database, so there are no transactions
        pass

    def commit(self) -> None:
        # there's no database, so there are no transactions
        pass

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        # there's no database, so just return the sql
        return AdapterResponse(_message="", code=sql), agate.Table([])
