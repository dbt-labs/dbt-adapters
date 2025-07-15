import re
import os

import time

import redshift_connector
import sqlparse

from multiprocessing.synchronize import RLock
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Tuple, Union, Optional, List, TYPE_CHECKING
from dataclasses import dataclass, field

from dbt.adapters.exceptions import FailedToConnectError
from redshift_connector.utils.oids import get_datatype_name

from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.redshift.auth_providers import create_token_service_client
from dbt_common.contracts.util import Replaceable
from dbt_common.dataclass_schema import dbtClassMixin, StrEnum, ValidationError
from dbt_common.helper_types import Port
from dbt_common.exceptions import DbtRuntimeError, CompilationError, DbtDatabaseError

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate

COMMENT_REGEX = re.compile(r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE)

logger = AdapterLogger("Redshift")

if os.getenv("DBT_REDSHIFT_CONNECTOR_DEBUG_LOGGING"):
    for logger_name in ["redshift_connector"]:
        logger.debug(f"Setting {logger_name} to DEBUG")
        logger.set_adapter_dependency_log_level(logger_name, "DEBUG")


class SSLConfigError(CompilationError):
    def __init__(self, exc: ValidationError):
        self.exc = exc
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        validator_msg = self.validator_error_message(self.exc)
        msg = f"Could not parse SSL config: {validator_msg}"
        return msg


logger = AdapterLogger("Redshift")


class RedshiftConnectionMethod(StrEnum):
    DATABASE = "database"
    IAM = "iam"
    IAM_ROLE = "iam_role"
    IAM_IDENTITY_CENTER_BROWSER = "browser_identity_center"
    IAM_IDENTITY_CENTER_TOKEN = "oauth_token_identity_center"

    @classmethod
    def uses_identity_center(cls, method: str) -> bool:
        return method in (cls.IAM_IDENTITY_CENTER_BROWSER, cls.IAM_IDENTITY_CENTER_TOKEN)

    @classmethod
    def is_iam(cls, method: str) -> bool:
        return not cls.uses_identity_center(method)


class UserSSLMode(StrEnum):
    disable = "disable"
    allow = "allow"
    prefer = "prefer"
    require = "require"
    verify_ca = "verify-ca"
    verify_full = "verify-full"

    @classmethod
    def default(cls) -> "UserSSLMode":
        # default for `psycopg2`, which aligns with dbt-redshift 1.4 and provides backwards compatibility
        return cls("prefer")


class RedshiftSSLMode(StrEnum):
    verify_ca = "verify-ca"
    verify_full = "verify-full"


SSL_MODE_TRANSLATION = {
    UserSSLMode.disable: None,
    UserSSLMode.allow: RedshiftSSLMode("verify-ca"),
    UserSSLMode.prefer: RedshiftSSLMode("verify-ca"),
    UserSSLMode.require: RedshiftSSLMode("verify-ca"),
    UserSSLMode.verify_ca: RedshiftSSLMode("verify-ca"),
    UserSSLMode.verify_full: RedshiftSSLMode("verify-full"),
}


@dataclass
class RedshiftSSLConfig(dbtClassMixin, Replaceable):
    ssl: bool = True
    sslmode: Optional[RedshiftSSLMode] = SSL_MODE_TRANSLATION[UserSSLMode.default()]

    @classmethod
    def parse(cls, user_sslmode: UserSSLMode) -> "RedshiftSSLConfig":
        try:
            raw_redshift_ssl = {
                "ssl": user_sslmode != UserSSLMode.disable,
                "sslmode": SSL_MODE_TRANSLATION[user_sslmode],
            }
            cls.validate(raw_redshift_ssl)
        except ValidationError as exc:
            raise SSLConfigError(exc)

        redshift_ssl = cls.from_dict(raw_redshift_ssl)

        if redshift_ssl.ssl:
            message = (
                f"Establishing connection using ssl with `sslmode` set to '{user_sslmode}'."
                f"To connect without ssl, set `sslmode` to 'disable'."
            )
        else:
            message = "Establishing connection without ssl."

        logger.debug(message)

        return redshift_ssl


@dataclass
class RedshiftCredentials(Credentials):
    host: str
    port: Port
    method: str = RedshiftConnectionMethod.DATABASE
    user: Optional[str] = None
    password: Optional[str] = None
    cluster_id: Optional[str] = field(
        default=None,
        metadata={"description": "If using IAM auth, the name of the cluster"},
    )
    iam_profile: Optional[str] = None
    autocreate: bool = False
    db_groups: List[str] = field(default_factory=list)
    ra3_node: Optional[bool] = False
    connect_timeout: Optional[int] = None
    role: Optional[str] = None
    sslmode: UserSSLMode = field(default_factory=UserSSLMode.default)
    retries: int = 1
    retry_all: bool = False
    region: Optional[str] = None
    # opt-in by default per team deliberation on https://peps.python.org/pep-0249/#autocommit
    autocommit: Optional[bool] = True
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None

    #
    # IAM identity center methods
    #

    # browser
    idc_region: Optional[str] = None
    issuer_url: Optional[str] = None
    idp_listen_port: Optional[int] = 7890
    idc_client_display_name: Optional[str] = "Amazon Redshift driver"
    idp_response_timeout: Optional[int] = None

    # token_endpoint
    #   a field that we expect to be a dictionary of values used to create
    #   access tokens from an external identity provider integrated with a redshift
    #   and aws org or account Iam Idc instance
    token_endpoint: Optional[Dict[str, str]] = None
    is_serverless: Optional[bool] = None
    serverless_work_group: Optional[str] = field(
        default=None,
        metadata={"description": "If using IAM auth, the name of the serverless workgroup"},
    )
    serverless_acct_id: Optional[str] = field(
        default=None,
        metadata={"description": "If using IAM auth, the AWS account id"},
    )

    # TCP keepalive settings
    tcp_keepalive: Optional[bool] = True
    tcp_keepalive_idle: Optional[int] = None
    tcp_keepalive_interval: Optional[int] = None
    tcp_keepalive_count: Optional[int] = None

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        return (
            "host",
            "user",
            "port",
            "database",
            "method",
            "cluster_id",
            "iam_profile",
            "schema",
            "sslmode",
            "region",
            "sslmode",
            "region",
            "autocreate",
            "db_groups",
            "ra3_node",
            "connect_timeout",
            "role",
            "retries",
            "retry_all",
            "autocommit",
            "access_key_id",
            "is_serverless",
            "serverless_work_group",
            "serverless_acct_id",
            "tcp_keepalive",
            "tcp_keepalive_idle",
            "tcp_keepalive_interval",
            "tcp_keepalive_count",
        )

    @property
    def unique_field(self) -> str:
        return self.host


def is_serverless(credentials: RedshiftCredentials) -> bool:
    return "serverless" in credentials.host or credentials.is_serverless is True


def get_connection_method(
    credentials: RedshiftCredentials,
) -> Callable[[], redshift_connector.Connection]:
    #
    # Helper Methods
    #
    def __validate_required_fields(method_name: str, required_fields: Tuple[str, ...]):
        missing_fields: List[str] = [
            field for field in required_fields if getattr(credentials, field, None) is None
        ]
        if missing_fields:
            fields_str: str = "', '".join(missing_fields)
            raise FailedToConnectError(
                f"'{fields_str}' field(s) are required for '{method_name}' credentials method"
            )

    def __base_kwargs(credentials) -> Dict[str, Any]:
        redshift_ssl_config: Dict[str, Any] = RedshiftSSLConfig.parse(
            credentials.sslmode
        ).to_dict()

        # Base connection parameters
        base_kwargs = {
            "host": credentials.host,
            "port": int(credentials.port) if credentials.port else 5439,
            "database": credentials.database,
            "region": credentials.region,
            "auto_create": credentials.autocreate,
            "db_groups": credentials.db_groups,
            "timeout": credentials.connect_timeout,
            "is_serverless": is_serverless(credentials),
            **redshift_ssl_config,
        }

        # Add TCP keepalive parameters if enabled
        if credentials.tcp_keepalive:
            base_kwargs["tcp_keepalive"] = True
            if credentials.tcp_keepalive_idle is not None:
                base_kwargs["tcp_keepalive_idle"] = credentials.tcp_keepalive_idle
            if credentials.tcp_keepalive_interval is not None:
                base_kwargs["tcp_keepalive_interval"] = credentials.tcp_keepalive_interval
            if credentials.tcp_keepalive_count is not None:
                base_kwargs["tcp_keepalive_count"] = credentials.tcp_keepalive_count

        return base_kwargs

    def __iam_kwargs(credentials) -> Dict[str, Any]:

        # iam True except for identity center methods
        iam: bool = RedshiftConnectionMethod.is_iam(credentials.method)
        cluster_identifier: Optional[str]
        serverless_work_group: Optional[str]
        serverless_acct_id: Optional[str]
        if RedshiftConnectionMethod.uses_identity_center(credentials.method):
            cluster_identifier = None
            serverless_work_group = None
            serverless_acct_id = None
        elif is_serverless(credentials):
            cluster_identifier = None
            serverless_work_group = credentials.serverless_work_group
            serverless_acct_id = credentials.serverless_acct_id
        elif credentials.cluster_id:
            cluster_identifier = credentials.cluster_id
            serverless_work_group = None
            serverless_acct_id = None
        else:
            raise FailedToConnectError(
                "Failed to use IAM method:"
                " 'cluster_id' must be provided for provisioned cluster"
                " 'host' must be provided for serverless endpoint"
            )

        iam_specific_kwargs: Dict[str, Any] = {
            "iam": iam,
            "user": "",
            "password": "",
            "cluster_identifier": cluster_identifier,
            "serverless_work_group": serverless_work_group,
            "serverless_acct_id": serverless_acct_id,
        }

        return __base_kwargs(credentials) | iam_specific_kwargs

    def __database_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with 'database' credentials method")

        __validate_required_fields("database", ("user", "password"))

        db_credentials: Dict[str, Any] = {
            "user": credentials.user,
            "password": credentials.password,
        }

        return __base_kwargs(credentials) | db_credentials

    def __iam_user_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with 'iam' credentials method")

        iam_credentials: Dict[str, Any]
        if credentials.access_key_id and credentials.secret_access_key:
            iam_credentials = {
                "access_key_id": credentials.access_key_id,
                "secret_access_key": credentials.secret_access_key,
            }
        elif credentials.access_key_id or credentials.secret_access_key:
            raise FailedToConnectError(
                "'access_key_id' and 'secret_access_key' are both needed if providing explicit credentials"
            )
        else:
            iam_credentials = {"profile": credentials.iam_profile}

        __validate_required_fields("iam", ("user",))
        iam_credentials["db_user"] = credentials.user

        return __iam_kwargs(credentials) | iam_credentials

    def __iam_role_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with 'iam_role' credentials method")
        role_kwargs = {
            "db_user": None,
            "group_federation": not is_serverless(credentials),
        }

        if credentials.iam_profile:
            role_kwargs["profile"] = credentials.iam_profile

        return __iam_kwargs(credentials) | role_kwargs

    def __iam_idc_browser_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with '{credentials.method}' credentials method")

        __IDP_TIMEOUT: int = 60
        __LISTEN_PORT_DEFAULT: int = 7890

        __validate_required_fields(
            "browser_identity_center", ("method", "idc_region", "issuer_url")
        )

        idp_timeout: int = (
            timeout
            if (timeout := credentials.idp_response_timeout) or timeout == 0
            else __IDP_TIMEOUT
        )

        idp_listen_port: int = (
            port if (port := credentials.idp_listen_port) else __LISTEN_PORT_DEFAULT
        )

        idc_kwargs: Dict[str, Any] = {
            "credentials_provider": "BrowserIdcAuthPlugin",
            "issuer_url": credentials.issuer_url,
            "listen_port": idp_listen_port,
            "idc_region": credentials.idc_region,
            "idc_client_display_name": credentials.idc_client_display_name,
            "idp_response_timeout": idp_timeout,
        }

        return __iam_kwargs(credentials) | idc_kwargs

    def __iam_idc_token_kwargs(credentials) -> Dict[str, Any]:
        """
        Accepts a `credentials` object with a `token_endpoint` field that corresponds to
        either Okta or Entra authentication services.

        We only support token_type=EXT_JWT tokens. token_type=ACCESS_TOKEN has not been
        tested. It can be added with a presenting use-case.
        """

        logger.debug("Connecting to Redshift with '{credentials.method}' credentials method")

        __validate_required_fields("oauth_token_identity_center", ("token_endpoint",))

        token_service = create_token_service_client(credentials.token_endpoint)
        response = token_service.handle_request()
        try:
            access_token = response.json()["access_token"]
        except KeyError:
            raise FailedToConnectError(
                "access_token missing from Idp token request. Please confirm correct configuration of the token_endpoint field in profiles.yml and that your Idp can use a refresh token to obtain an OIDC-compliant access token."
            )

        return __iam_kwargs(credentials) | {
            "credentials_provider": "IdpTokenAuthPlugin",
            "token": access_token,
            "token_type": "EXT_JWT",
        }

    #
    # Head of function execution
    #

    method_to_kwargs_function = {
        None: __database_kwargs,
        RedshiftConnectionMethod.DATABASE: __database_kwargs,
        RedshiftConnectionMethod.IAM: __iam_user_kwargs,
        RedshiftConnectionMethod.IAM_ROLE: __iam_role_kwargs,
        RedshiftConnectionMethod.IAM_IDENTITY_CENTER_BROWSER: __iam_idc_browser_kwargs,
        RedshiftConnectionMethod.IAM_IDENTITY_CENTER_TOKEN: __iam_idc_token_kwargs,
    }

    try:
        kwargs_function: Callable[[RedshiftCredentials], Dict[str, Any]] = (
            method_to_kwargs_function[credentials.method]
        )
    except KeyError:
        raise FailedToConnectError(f"Invalid 'method' in profile: '{credentials.method}'")

    kwargs: Dict[str, Any] = kwargs_function(credentials)

    def connect() -> redshift_connector.Connection:
        c = redshift_connector.connect(**kwargs)
        if credentials.autocommit:
            c.autocommit = True
        if credentials.role:
            c.cursor().execute(f"set role {credentials.role}")
        return c

    return connect


class RedshiftConnectionManager(SQLConnectionManager):
    TYPE = "redshift"

    def cancel(self, connection: Connection):
        pid = connection.backend_pid
        sql = f"select pg_terminate_backend({pid})"
        logger.debug(f"Cancel query on: '{connection.name}' with PID: {pid}")
        logger.debug(sql)

        try:
            self.add_query(sql)
        except redshift_connector.InterfaceError as e:
            if "is closed" in str(e):
                logger.debug(f"Connection {connection.name} was already closed")
                return
            raise

    @classmethod
    def _get_backend_pid(cls, connection):
        with connection.handle.cursor() as c:
            sql = "select pg_backend_pid()"
            res = c.execute(sql).fetchone()
        if res:
            return res[0]
        return None

    @classmethod
    def get_response(cls, cursor: redshift_connector.Cursor) -> AdapterResponse:
        # redshift_connector.Cursor doesn't have a status message attribute but
        # this function is only used for successful run, so we can just return a dummy
        rows = cursor.rowcount
        message = "SUCCESS"
        return AdapterResponse(_message=message, rows_affected=rows)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except redshift_connector.DatabaseError as e:
            try:
                err_msg = e.args[0]["M"]  # this is a type redshift sets, so we must use these keys
            except Exception:
                err_msg = str(e).strip()
            logger.debug(f"Redshift error: {err_msg}")
            self.rollback_if_open()
            raise DbtDatabaseError(err_msg) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            # Raise DBT native exceptions as is.
            if isinstance(e, DbtRuntimeError):
                raise
            raise DbtRuntimeError(str(e)) from e

    @contextmanager
    def fresh_transaction(self) -> Generator[None, None, None]:
        """On entrance to this context manager, hold an exclusive lock and
        create a fresh transaction for redshift, then commit and begin a new
        one before releasing the lock on exit.

        See drop_relation in RedshiftAdapter for more information.
        """
        drop_lock: RLock = self.lock

        with drop_lock:
            connection = self.get_thread_connection()

            if connection.transaction_open:
                self.commit()

            self.begin()
            yield
            self.commit()

            self.begin()

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        retryable_exceptions = (
            redshift_connector.OperationalError,
            redshift_connector.DatabaseError,
            redshift_connector.DataError,
            redshift_connector.InterfaceError,
        )
        if credentials.retry_all:
            retryable_exceptions = redshift_connector.Error

        open_connection = cls.retry_connection(
            connection,
            connect=get_connection_method(credentials),
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

        if backend_pid := cls._get_backend_pid(open_connection):
            open_connection.backend_pid = backend_pid
        return open_connection

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        def _handle_execute_exception(
            e: Exception, retries: int, backoff: int, retry_all: bool
        ) -> Tuple[int, int]:
            oid_not_found_msg = "could not open relation with OID"
            if retries == 0:
                raise e
            if oid_not_found_msg in str(e):
                pass
            elif isinstance(e, DbtDatabaseError) and retry_all:
                pass
            else:
                logger.debug(f"Not retrying error: {e}")
                raise e
            logger.debug(f"Retrying query due to error: {e}")
            retries -= 1
            # we need to actually close and open to get a new connection
            # otherwise no queries will succeed on this connection
            self.close(self.get_thread_connection())
            self.open(self.get_thread_connection())
            time.sleep(backoff)
            # return with exponential backoff
            return retries, min(max(backoff * 2, 2), 60)

        def _execute_internal(
            sql: str,
            auto_begin: bool,
            fetch: bool,
            limit: Optional[int],
            retries: int,
            backoff: int,
            retry_all: bool,
        ) -> Tuple[Optional[AdapterResponse], Optional["agate.Table"]]:
            try:
                _, cursor = self.add_query(sql, auto_begin)
                internal_response = self.get_response(cursor)
                if fetch:
                    internal_table = self.get_result_from_cursor(cursor, limit)
                else:
                    from dbt_common.clients import agate_helper

                    internal_table = agate_helper.empty_table()
                return internal_response, internal_table
            except Exception as e:
                retries, backoff = _handle_execute_exception(e, retries, backoff, retry_all)
                return _execute_internal(
                    sql, auto_begin, fetch, limit, retries, backoff, retry_all
                )

        response, table = _execute_internal(
            self._add_query_comment(sql),
            auto_begin,
            fetch,
            limit,
            self.profile.credentials.retries,
            1,
            self.profile.credentials.retry_all,
        )

        # this should never happen but mypy doesn't know that and arguably neither do we
        if response is None or table is None:
            conn_name = thread_conn.name if (thread_conn := self.get_if_exists()) else "<None>"
            raise DbtRuntimeError(f"Failed to execute SQL: {sql} on connection: {conn_name}")
        return response, table

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):  # type: ignore
        connection = None
        cursor = None
        self._initialize_sqlparse_lexer()

        queries = sqlparse.split(sql)

        redshift_retryable_exceptions = (
            redshift_connector.InterfaceError,
            redshift_connector.InternalError,
        )

        for query in queries:
            # Strip off comments from the current query
            without_comments = re.sub(
                COMMENT_REGEX,
                "",
                query,
            ).strip()

            if without_comments == "":
                continue

            connection, cursor = super().add_query(
                query,
                auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
                retryable_exceptions=redshift_retryable_exceptions,
                retry_limit=self.profile.credentials.retries,
            )

        if cursor is None:
            conn = self.get_thread_connection()
            conn_name = conn.name if conn and conn.name else "<None>"
            raise DbtRuntimeError(f"Tried to run invalid SQL: {sql} on {conn_name}")

        return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        return get_datatype_name(type_code)

    @staticmethod
    def _initialize_sqlparse_lexer():
        """
        Resolves: https://github.com/dbt-labs/dbt-redshift/issues/710
        Implementation of this fix: https://github.com/dbt-labs/dbt-core/pull/8215
        """
        from sqlparse.lexer import Lexer

        if hasattr(Lexer, "get_default_instance"):
            Lexer.get_default_instance()
