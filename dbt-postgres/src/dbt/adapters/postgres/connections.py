from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Union

from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import TypeCodeNotFound
from dbt.adapters.postgres.record import PostgresRecordReplayHandle
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.events.functions import warn_or_error
from dbt_common.helper_types import Port
from dbt_common.record import get_record_mode_from_env, RecorderMode
from mashumaro.jsonschema.annotations import Maximum, Minimum
import psycopg2
from typing_extensions import Annotated


logger = AdapterLogger("Postgres")


# PostgreSQL built-in types that psycopg2 does not register a typecaster for, keyed by the type
# OID a cursor description reports and valued by the SQL name `format_type(oid, null)` returns.
#
# psycopg2 can only name the OIDs it has a typecaster for (`psycopg2.extensions.string_types`),
# and `uuid`, `money`, `inet`, `bit`, the geometric types and the rest below are not among them.
# Contract enforcement therefore could not name those columns and fell through to
# `unknown type_code <oid>` plus a TypeCodeNotFound event (dbt-labs/dbt-core#8353, #8877,
# #8900), which under `--warn-error` fails every contracted model with such a column.
#
# The driver is still consulted first, so every type psycopg2 does name keeps the name it had.
# This table is only the fallback for the built-ins it does not, plus their array types.
#
# Generated from `pg_catalog.pg_type` on PostgreSQL 16.13: `typtype = 'b'`, excluding the
# pseudo, polymorphic and internal-use categories, minus the OIDs psycopg2 2.9.13 registers.
# tests/functional/test_type_oid_mapping.py checks every entry against the live catalog.
TYPE_OID_TO_DATA_TYPE: Dict[int, str] = {
    22: "int2vector",
    24: "regproc",
    27: "tid",
    28: "xid",
    29: "cid",
    30: "oidvector",
    142: "xml",
    143: "xml[]",
    271: "xid8[]",
    600: "point",
    601: "lseg",
    602: "path",
    603: "box",
    604: "polygon",
    628: "line",
    629: "line[]",
    650: "cidr",
    718: "circle",
    719: "circle[]",
    774: "macaddr8",
    775: "macaddr8[]",
    790: "money",
    791: "money[]",
    829: "macaddr",
    869: "inet",
    1008: "regproc[]",
    1010: "tid[]",
    1011: "xid[]",
    1012: "cid[]",
    1017: "point[]",
    1018: "lseg[]",
    1019: "path[]",
    1020: "box[]",
    1027: "polygon[]",
    1033: "aclitem",
    1034: "aclitem[]",
    1560: "bit",
    1561: "bit[]",
    1562: "bit varying",  # varbit
    1563: "bit varying[]",  # varbit[]
    1790: "refcursor",
    2201: "refcursor[]",
    2202: "regprocedure",
    2203: "regoper",
    2204: "regoperator",
    2205: "regclass",
    2206: "regtype",
    2207: "regprocedure[]",
    2208: "regoper[]",
    2209: "regoperator[]",
    2210: "regclass[]",
    2211: "regtype[]",
    2949: "txid_snapshot[]",
    2950: "uuid",
    2951: "uuid[]",
    2970: "txid_snapshot",
    3220: "pg_lsn",
    3221: "pg_lsn[]",
    3614: "tsvector",
    3615: "tsquery",
    3642: "gtsvector",
    3643: "tsvector[]",
    3644: "gtsvector[]",
    3645: "tsquery[]",
    3734: "regconfig",
    3735: "regconfig[]",
    3769: "regdictionary",
    3770: "regdictionary[]",
    4072: "jsonpath",
    4073: "jsonpath[]",
    4089: "regnamespace",
    4090: "regnamespace[]",
    4096: "regrole",
    4097: "regrole[]",
    4191: "regcollation",
    4192: "regcollation[]",
    5038: "pg_snapshot",
    5039: "pg_snapshot[]",
    5069: "xid8",
}


@dataclass
class PostgresCredentials(Credentials):
    host: str
    user: str
    # Annotated is used by mashumaro for jsonschema generation
    port: Annotated[Port, Minimum(0), Maximum(65535)]
    password: str  # on postgres the password is mandatory
    connect_timeout: int = 10
    role: Optional[str] = None
    search_path: Optional[str] = None
    keepalives_idle: int = 0  # 0 means to use the default value
    sslmode: Optional[str] = None
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None
    application_name: Optional[str] = "dbt"
    autocommit: Optional[bool] = False
    retries: int = 1

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "postgres"

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
            "keepalives_idle",
            "sslmode",
            "sslcert",
            "sslkey",
            "sslrootcert",
            "application_name",
            "autocommit",
            "retries",
        )


class PostgresConnectionManager(SQLConnectionManager):
    TYPE = "postgres"

    def __init__(self, profile, mp_context):
        super().__init__(profile, mp_context)
        self._skip_transactions_checker: Optional[Callable[[], bool]] = None

    def set_skip_transactions_checker(self, checker: Callable[[], bool]) -> None:
        self._skip_transactions_checker = checker

    def _is_autocommit_enabled(self) -> bool:
        """Check if autocommit is enabled for the current connection."""
        connection = self.get_thread_connection()
        return connection.credentials.autocommit is True

    def _should_skip_transaction_statements(self) -> bool:
        """Check if we should skip BEGIN/COMMIT/ROLLBACK statements.

        Returns True if:
        1. autocommit is enabled (each statement auto-commits)
        2. The behavior flag is set (checked via _skip_transactions_checker)

        Both conditions must be true to skip transaction statements.
        """
        if not self._is_autocommit_enabled():
            return False

        if self._skip_transactions_checker is None:
            return False

        return self._skip_transactions_checker()

    def begin(self):
        connection = self.get_thread_connection()

        if not self._should_skip_transaction_statements():
            super().begin()

        connection.transaction_open = True

    def commit(self):
        connection = self.get_thread_connection()

        if not self._should_skip_transaction_statements():
            super().commit()
        connection.transaction_open = False

    def rollback_if_open(self):
        connection = self.get_thread_connection()

        if not self._should_skip_transaction_statements():
            super().rollback_if_open()
        connection.transaction_open = False

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug("Postgres error: {}".format(str(e)))

            try:
                self.rollback_if_open()
            except psycopg2.Error:
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
        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if credentials.keepalives_idle:
            kwargs["keepalives_idle"] = credentials.keepalives_idle

        # psycopg2 doesn't support search_path officially,
        # see https://github.com/psycopg/psycopg2/issues/465
        search_path = credentials.search_path
        if search_path is not None and search_path != "":
            # see https://postgresql.org/docs/9.5/libpq-connect.html
            kwargs["options"] = "-c search_path={}".format(search_path.replace(" ", "\\ "))

        if credentials.sslmode:
            kwargs["sslmode"] = credentials.sslmode

        if credentials.sslcert is not None:
            kwargs["sslcert"] = credentials.sslcert

        if credentials.sslkey is not None:
            kwargs["sslkey"] = credentials.sslkey

        if credentials.sslrootcert is not None:
            kwargs["sslrootcert"] = credentials.sslrootcert

        if credentials.application_name:
            kwargs["application_name"] = credentials.application_name

        def connect():
            handle = None

            # In replay mode, we won't connect to a real database at all, while
            # in record and diff modes we do, but insert an intermediate handle
            # object which monitors native connection activity.
            rec_mode = get_record_mode_from_env()
            if rec_mode != RecorderMode.REPLAY:
                handle = psycopg2.connect(
                    dbname=credentials.database,
                    user=credentials.user,
                    host=credentials.host,
                    password=credentials.password,
                    port=credentials.port,
                    connect_timeout=credentials.connect_timeout,
                    **kwargs,
                )

            if handle is not None and credentials.autocommit:
                handle.autocommit = True

            if rec_mode is not None:
                # If using the record/replay mechanism, regardless of mode, we
                # use a wrapper.
                handle = PostgresRecordReplayHandle(handle, connection)

            if credentials.role:
                handle.cursor().execute("set role {}".format(credentials.role))

            return handle

        retryable_exceptions = [
            # OperationalError is subclassed by all psycopg2 Connection Exceptions and it's raised
            # by generic connection timeouts without an error code. This is a limitation of
            # psycopg2 which doesn't provide subclasses for errors without a SQLSTATE error code.
            # The limitation has been known for a while and there are no efforts to tackle it.
            # See: https://github.com/psycopg/psycopg2/issues/682
            psycopg2.errors.OperationalError,
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
            pid = connection.handle.get_backend_pid()
        except psycopg2.InterfaceError as exc:
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
        message = str(cursor.statusmessage)
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_messsage_strings = [part for part in status_message_parts if not part.isdigit()]
        code = " ".join(status_messsage_strings)
        return AdapterResponse(_message=message, code=code, rows_affected=rows)

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        if type_code in psycopg2.extensions.string_types:
            return psycopg2.extensions.string_types[type_code].name
        if type_code in TYPE_OID_TO_DATA_TYPE:
            return TYPE_OID_TO_DATA_TYPE[type_code]
        warn_or_error(TypeCodeNotFound(type_code=type_code))
        return f"unknown type_code {type_code}"
