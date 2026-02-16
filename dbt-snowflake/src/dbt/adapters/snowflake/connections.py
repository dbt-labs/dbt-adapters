import base64
import re
from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep

from typing import Optional, Tuple, Union, Any, List, TYPE_CHECKING, Dict

import requests
from adbc_driver_snowflake import dbapi as snowflake_dbapi
from adbc_driver_manager import dbapi as adbc_dbapi

from dbt_common.exceptions import (
    DbtInternalError,
    DbtRuntimeError,
    DbtConfigError,
)
from dbt_common.exceptions import DbtDatabaseError
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.events.functions import warn_or_error
from dbt.adapters.events.types import AdapterEventWarning, AdapterEventError
from dbt_common.ui import line_wrap_message, warning_tag

from dbt.adapters.snowflake.auth import private_key_to_pem_string
from dbt.adapters.snowflake.query_headers import SnowflakeMacroQueryStringSetter

if TYPE_CHECKING:
    import agate


logger = AdapterLogger("Snowflake")


_TOKEN_REQUEST_URL = "https://{}.snowflakecomputing.com/oauth/token-request"

ERROR_REDACTION_PATTERNS = {
    re.compile(r"Row Values: \[(.|\n)*\]"): "Row Values: [redacted]",
    re.compile(r"Duplicate field key '(.|\n)*'"): "Duplicate field key '[redacted]'",
}


@dataclass
class SnowflakeCredentials(Credentials):
    account: str
    user: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    password: Optional[str] = None
    authenticator: Optional[str] = None
    private_key: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    token: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    query_tag: Optional[str] = None
    client_session_keep_alive: bool = False
    host: Optional[str] = None
    port: Optional[int] = None
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    protocol: Optional[str] = None
    connect_retries: int = 1
    connect_timeout: Optional[int] = None
    retry_on_database_errors: bool = False
    retry_all: bool = False
    insecure_mode: Optional[bool] = False
    # this needs to default to `None` so that we can tell if the user set it; see `__post_init__()`
    reuse_connections: Optional[bool] = None
    s3_stage_vpce_dns_name: Optional[str] = None
    # Setting this to 0.0 will disable platform detection which adds query latency
    # this should only be set to a non-zero value if you are using WIF authentication
    platform_detection_timeout_seconds: float = 0.0

    def __post_init__(self):
        if self.authenticator != "oauth" and (self.oauth_client_secret or self.oauth_client_id):
            # the user probably forgot to set 'authenticator' like I keep doing
            warn_or_error(
                AdapterEventWarning(
                    base_msg="Authenticator is not set to oauth, but an oauth-only parameter is set! Did you mean to set authenticator: oauth?"
                )
            )

        if self.authenticator not in ["oauth", "jwt"]:
            if self.token:
                warn_or_error(
                    AdapterEventWarning(
                        base_msg=(
                            "The token parameter was set, but the authenticator was "
                            "not set to 'oauth' or 'jwt'."
                        )
                    )
                )

            if not self.user:
                # The user attribute is only optional if 'authenticator' is 'jwt' or 'oauth'
                warn_or_error(
                    AdapterEventError(base_msg="Invalid profile: 'user' is a required property.")
                )

        self.account, sub_count = re.subn("_", "-", self.account)
        if sub_count:
            logger.debug(
                "Replaced underscores (_) with hyphens (-) in Snowflake account name to form a valid account URL."
            )

        # only default `reuse_connections` to `True` if the user has not turned on `client_session_keep_alive`
        # having both of these set to `True` could lead to hanging open connections, so it should be opt-in behavior
        if self.client_session_keep_alive is False and self.reuse_connections is None:
            self.reuse_connections = True

    @property
    def type(self):
        return "snowflake"

    @property
    def unique_field(self):
        return self.account

    # the results show up in the output of dbt debug runs, for more see..
    # https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/3-building-a-new-adapter#editing-the-connection-manager
    def _connection_keys(self):
        return (
            "account",
            "user",
            "database",
            "warehouse",
            "role",
            "schema",
            "authenticator",
            "oauth_client_id",
            "query_tag",
            "client_session_keep_alive",
            "host",
            "port",
            "proxy_host",
            "proxy_port",
            "protocol",
            "connect_retries",
            "connect_timeout",
            "retry_on_database_errors",
            "retry_all",
            "insecure_mode",
            "reuse_connections",
            "s3_stage_vpce_dns_name",
            "platform_detection_timeout_seconds",
        )

    def adbc_auth_args(self) -> Dict[str, str]:
        """Build db_kwargs dict for adbc_driver_snowflake.dbapi.connect()."""
        result: Dict[str, str] = {}

        result["adbc.snowflake.sql.account"] = self.account
        if self.user:
            result["username"] = self.user
        if self.password:
            result["password"] = self.password
        if self.warehouse:
            result["adbc.snowflake.sql.warehouse"] = self.warehouse
        if self.database:
            result["adbc.snowflake.sql.db"] = self.database
        if self.schema:
            result["adbc.snowflake.sql.schema"] = self.schema
        if self.role:
            result["adbc.snowflake.sql.role"] = self.role

        if self.authenticator:
            auth_type_map = {
                "externalbrowser": "auth_ext_browser",
                "oauth": "auth_oauth",
                "jwt": "auth_jwt",
            }
            adbc_auth_type = auth_type_map.get(self.authenticator)
            if adbc_auth_type:
                result["adbc.snowflake.sql.auth_type"] = adbc_auth_type

            if self.authenticator == "oauth":
                token = self.token
                if self.oauth_client_id and self.oauth_client_secret:
                    token = self._get_access_token()
                elif self.oauth_client_id:
                    warn_or_error(
                        AdapterEventWarning(
                            base_msg="Invalid profile: got an oauth_client_id, but not an oauth_client_secret!"
                        )
                    )
                elif self.oauth_client_secret:
                    warn_or_error(
                        AdapterEventWarning(
                            base_msg="Invalid profile: got an oauth_client_secret, but not an oauth_client_id!"
                        )
                    )
                if token:
                    result["adbc.snowflake.sql.client_option.auth_token"] = token

            elif self.authenticator == "jwt":
                if self.token:
                    result["adbc.snowflake.sql.client_option.auth_token"] = self.token

        # Private key auth
        if self.private_key and self.private_key_path:
            raise DbtConfigError("Cannot specify both `private_key` and `private_key_path`")
        elif self.private_key:
            pem_str = private_key_to_pem_string(self.private_key, self.private_key_passphrase)
            result["adbc.snowflake.sql.client_option.jwt_private_key"] = pem_str
        elif self.private_key_path:
            result["adbc.snowflake.sql.client_option.jwt_private_key"] = self.private_key_path

        return result

    def auth_args(self):
        """Deprecated: use adbc_auth_args() instead. Kept for reference."""
        return self.adbc_auth_args()

    def _get_access_token(self) -> str:
        if self.authenticator != "oauth":
            raise DbtInternalError("Can only get access tokens for oauth")
        missing = any(
            x is None for x in (self.oauth_client_id, self.oauth_client_secret, self.token)
        )
        if missing:
            raise DbtInternalError(
                "need a client ID a client secret, and a refresh token to get an access token"
            )

        # should the full url be a config item?
        token_url = _TOKEN_REQUEST_URL.format(self.account)
        # I think this is only used to redirect on success, which we ignore
        # (it does not have to match the integration's settings in snowflake)
        redirect_uri = "http://localhost:9999"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.token,
            "redirect_uri": redirect_uri,
        }

        auth = base64.b64encode(
            f"{self.oauth_client_id}:{self.oauth_client_secret}".encode("ascii")
        ).decode("ascii")
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-type": "application/x-www-form-urlencoded;charset=utf-8",
        }

        result_json = None
        max_iter = 20
        # Attempt to obtain JSON for 1 second before throwing an error
        for i in range(max_iter):
            result = requests.post(token_url, headers=headers, data=data)
            try:
                result_json = result.json()
                break
            except ValueError as e:
                message = result.text
                logger.debug(
                    f"Got a non-json response ({result.status_code}): "
                    f"{e}, message: {message}"
                )
                sleep(0.05)

        if result_json is None:
            raise DbtDatabaseError(
                f"Did not receive valid json with access_token. "
                f"Showing json response: {result_json}"
            )
        elif "access_token" not in result_json:
            raise FailedToConnectError(
                "This error occurs when authentication has expired. "
                "Please reauth with your auth provider."
            )
        return result_json["access_token"]


class SnowflakeConnectionManager(SQLConnectionManager):
    TYPE = "snowflake"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except adbc_dbapi.ProgrammingError as e:
            msg = str(e)

            # Redact sensitive data from error messages
            for regex_pattern, replacement_message in ERROR_REDACTION_PATTERNS.items():
                msg = re.sub(regex_pattern, replacement_message, msg)

            logger.debug("Snowflake error: {}".format(msg))

            if "Empty SQL statement" in msg:
                logger.debug("got empty sql statement, moving on")
            elif "This session does not have a current database" in msg:
                raise FailedToConnectError(
                    (
                        "{}\n\nThis error sometimes occurs when invalid "
                        "credentials are provided, or when your default role "
                        "does not have access to use the specified database. "
                        "Please double check your profile and try again."
                    ).format(msg)
                )
            else:
                raise DbtDatabaseError(msg)
        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        timeout = creds.connect_timeout

        def connect():
            db_kwargs = creds.adbc_auth_args()
            handle = snowflake_dbapi.connect(db_kwargs=db_kwargs, autocommit=True)

            # Set session parameters post-connect via ALTER SESSION
            session_params = {}
            if creds.query_tag:
                session_params["QUERY_TAG"] = creds.query_tag
            if creds.s3_stage_vpce_dns_name:
                session_params["S3_STAGE_VPCE_DNS_NAME"] = creds.s3_stage_vpce_dns_name

            if session_params:
                cursor = handle.cursor()
                for key, value in session_params.items():
                    cursor.execute(f"ALTER SESSION SET {key} = '{value}'")
                cursor.close()

            return handle

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [
            adbc_dbapi.OperationalError,
            adbc_dbapi.InterfaceError,
        ]
        # these two options are for backwards compatibility
        if creds.retry_all:
            retryable_exceptions = [adbc_dbapi.Error]
        elif creds.retry_on_database_errors:
            retryable_exceptions.insert(0, adbc_dbapi.DatabaseError)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=creds.connect_retries,
            retry_timeout=timeout if timeout is not None else exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        logger.debug("Cancel query (no-op for ADBC — session_id not available)")

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        rows = cursor.rowcount if hasattr(cursor, "rowcount") else -1
        return AdapterResponse(
            _message=f"OK {rows}",
            rows_affected=rows,
            code="OK",
        )

    # disable transactional logic by default on Snowflake
    # except for DML statements where explicitly defined
    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @classmethod
    def _split_queries(cls, sql):
        """Splits sql statements at semicolons into discrete queries.

        Simple quote-aware splitter (replaces snowflake.connector.util_text.split_statements).
        """
        sql_s = str(sql)
        queries = []
        current = []
        in_single_quote = False
        in_double_quote = False
        i = 0
        while i < len(sql_s):
            c = sql_s[i]
            if c == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
                current.append(c)
            elif c == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
                current.append(c)
            elif c == ';' and not in_single_quote and not in_double_quote:
                query = ''.join(current).strip()
                if query:
                    queries.append(query)
                current = []
            else:
                current.append(c)
            i += 1
        # trailing query without semicolon
        query = ''.join(current).strip()
        if query:
            queries.append(query)
        return queries

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        # don't apply the query comment here
        # it will be applied after ';' queries are split
        from dbt_common.clients.agate_helper import empty_table

        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        return response, table

    def add_standard_query(self, sql: str, **kwargs) -> Tuple[Connection, Any]:
        # This is the happy path for a single query. Snowflake has a few odd behaviors that
        # require preprocessing within the 'add_query' method below.
        return super().add_query(self._add_query_comment(sql), **kwargs)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        *args,
        **kwargs,
    ) -> Tuple[Connection, Any]:
        if bindings:
            raise DbtRuntimeError(
                "ADBC driver does not support parameterized queries (bindings). "
                "Use inline values instead (e.g. in seed macros)."
            )

        stripped_queries = self._stripped_queries(sql)

        if set(query.lower() for query in stripped_queries).issubset({"begin;", "commit;"}):
            connection, cursor = self._add_begin_commit_only_queries(
                stripped_queries,
                auto_begin=auto_begin,
                abridge_sql_log=abridge_sql_log,
            )
        else:
            connection, cursor = self._add_standard_queries(
                stripped_queries,
                auto_begin=auto_begin,
                abridge_sql_log=abridge_sql_log,
            )

        if cursor is None:
            self._raise_cursor_not_found_error(sql)

        return connection, cursor

    def set_query_header(self, query_header_context: Dict[str, Any]) -> None:
        self.query_header = SnowflakeMacroQueryStringSetter(self.profile, query_header_context)

    def _stripped_queries(self, sql: str) -> List[str]:
        def strip_query(query):
            """
            hack -- after the last ';', remove comments and don't run
            empty queries. this avoids using exceptions as flow control,
            and also allows us to return the status of the last cursor
            """
            without_comments_re = re.compile(
                r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE
            )
            return re.sub(without_comments_re, "", query).strip()

        return [query for query in self._split_queries(sql) if strip_query(query) != ""]

    def _add_begin_commit_only_queries(
        self, queries: List[str], **kwargs
    ) -> Tuple[Connection, Any]:
        # if all we get is `begin;` and/or `commit;`
        # raise a warning, then run as standard queries to avoid an error downstream
        message = (
            "Explicit transactional logic should be used only to wrap "
            "DML logic (MERGE, DELETE, UPDATE, etc). The keywords BEGIN; and COMMIT; should "
            "be placed directly before and after your DML statement, rather than in separate "
            "statement calls or run_query() macros."
        )
        logger.warning(line_wrap_message(warning_tag(message)))

        for query in queries:
            connection, cursor = self.add_standard_query(query, **kwargs)
        return connection, cursor

    def _add_standard_queries(self, queries: List[str], **kwargs) -> Tuple[Connection, Any]:
        for query in queries:
            # Even though we turn off transactions by default for Snowflake,
            # the user/macro has passed them *explicitly*, probably to wrap a DML statement
            # This also has the effect of ignoring "commit" in the RunResult for this model
            # https://github.com/dbt-labs/dbt-snowflake/issues/147
            if query.lower() == "begin;":
                super().add_begin_query()
            elif query.lower() == "commit;":
                super().add_commit_query()
            else:
                # This adds a query comment to *every* statement
                # https://github.com/dbt-labs/dbt-snowflake/issues/140
                connection, cursor = self.add_standard_query(query, **kwargs)
        return connection, cursor

    def _raise_cursor_not_found_error(self, sql: str):
        conn = self.get_thread_connection()
        try:
            conn_name = conn.name
        except AttributeError:
            conn_name = None

        raise DbtRuntimeError(
            f"""Tried to run an empty query on model '{conn_name or "<None>"}'. If you are """
            f"""conditionally running\nsql, e.g. in a model hook, make """
            f"""sure your `else` clause contains valid sql!\n\n"""
            f"""Provided SQL:\n{sql}"""
        )

    def release(self):
        """Reuse connections by deferring release until adapter context manager in core
        resets adapters. This cleanup_all happens before Python teardown. Idle connections
        incur no costs while waiting in the connection pool."""
        if self.profile.credentials.reuse_connections:
            return
        super().release()

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        """Map ADBC/Arrow type codes to Snowflake type names."""
        code = str(type_code).lower()

        if code in ("int8", "int16", "int32", "int64"):
            return "NUMBER"
        if code in ("float", "float32"):
            return "FLOAT"
        if code in ("double", "float64"):
            return "FLOAT"
        if code in ("string", "large_string", "utf8", "large_utf8"):
            return "VARCHAR"
        if code == "bool":
            return "BOOLEAN"
        if code.startswith("decimal"):
            return "NUMBER"
        if code.startswith("date"):
            return "DATE"
        if code.startswith("timestamp"):
            return "TIMESTAMP_NTZ"
        if code == "binary" or code == "large_binary":
            return "BINARY"

        return str(type_code)
