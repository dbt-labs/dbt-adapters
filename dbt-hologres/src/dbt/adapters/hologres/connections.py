from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import TypeCodeNotFound
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.events.functions import warn_or_error
from dbt_common.helper_types import Port
from mashumaro.jsonschema.annotations import Maximum, Minimum
import psycopg
from typing_extensions import Annotated

from dbt.adapters.hologres.__version__ import version


logger = AdapterLogger("Hologres")


@dataclass
class HologresCredentials(Credentials):
    host: str
    user: str
    # Annotated is used by mashumaro for jsonschema generation
    port: Annotated[Port, Minimum(0), Maximum(65535)] = 80
    password: str  # on hologres the password is mandatory
    connect_timeout: int = 10
    role: Optional[str] = None
    search_path: Optional[str] = None
    sslmode: Optional[str] = "disable"  # Default to disable for Hologres
    application_name: Optional[str] = None
    retries: int = 1

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "hologres"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return (
            "host",
            "port",
            "user",
            "database",
            "schema",
            "connect_timeout",
            "role",
            "search_path",
            "sslmode",
            "application_name",
            "retries",
        )

    def __post_init__(self):
        # Set application_name with version if not provided
        if not self.application_name:
            self.application_name = f"dbt_hologres_{version}"


class HologresConnectionManager(SQLConnectionManager):
    TYPE = "hologres"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except psycopg.DatabaseError as e:
            logger.debug("Hologres error: {}".format(str(e)))

            try:
                self.rollback_if_open()
            except psycopg.Error:
                logger.debug("Failed to release connection!")
                pass

            raise DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise DbtRuntimeError(e) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}
        
        # psycopg3 doesn't support search_path through options
        # We'll set it after connection is established
        search_path = credentials.search_path
        
        if credentials.sslmode:
            kwargs["sslmode"] = credentials.sslmode

        if credentials.application_name:
            kwargs["application_name"] = credentials.application_name

        def connect():
            handle = psycopg.connect(
                dbname=credentials.database,
                user=credentials.user,
                host=credentials.host,
                password=credentials.password,
                port=credentials.port,
                connect_timeout=credentials.connect_timeout,
                **kwargs,
            )
            
            # Set search_path if specified
            if search_path is not None and search_path != "":
                with handle.cursor() as cursor:
                    cursor.execute(f"SET search_path TO {search_path}")
            
            # Set role if specified
            if credentials.role:
                with handle.cursor() as cursor:
                    cursor.execute(f"SET ROLE {credentials.role}")

            return handle

        retryable_exceptions = [
            # OperationalError is raised by connection failures
            psycopg.OperationalError,
        ]

        def exponential_backoff(attempt: int):
            return attempt * attempt

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        connection_name = connection.name
        try:
            pid = connection.handle.info.backend_pid
        except psycopg.InterfaceError as exc:
            # if the connection is already closed, not much to cancel!
            if "already closed" in str(exc):
                logger.debug(f"Connection {connection_name} was already closed")
                return
            # probably bad, re-raise it
            raise

        sql = "select pg_terminate_backend({})".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    def add_begin_query(self):
        pass

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        message = cursor.statusmessage if hasattr(cursor, 'statusmessage') else ""
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_message_strings = [part for part in status_message_parts if not part.isdigit()]
        code = " ".join(status_message_strings)
        return AdapterResponse(_message=message, code=code, rows_affected=rows)

    @classmethod
    def data_type_code_to_name(cls, type_code: int) -> str:
        # In psycopg3, we need to use TypeInfo to get type names
        # For now, return a simple mapping or unknown
        # This would need to be enhanced with actual type mapping
        try:
            # Basic type code to name mapping
            # This is a simplified version, full implementation would query pg_type
            return f"type_{type_code}"
        except Exception:
            warn_or_error(TypeCodeNotFound(type_code=type_code))
            return f"unknown type_code {type_code}"
