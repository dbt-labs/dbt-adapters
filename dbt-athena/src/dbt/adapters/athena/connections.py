import json
import re
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address, ip_address
from time import sleep
from typing import (
    Any,
    Callable,
    cast,
    ContextManager,
    Deque,
    Dict,
    List,
    Optional,
    Self,
    Tuple,
    TypeAlias,
    Union,
)
from uuid import UUID

from boto3.session import Session as BotoSession
from botocore.config import Config as BotoConfig
from dbt_common.exceptions import ConnectionError, DbtRuntimeError
from dbt_common.utils import md5
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

from dbt.adapters.athena.config import get_boto3_config
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.query_headers import AthenaMacroQueryStringSetter
from dbt.adapters.athena.session import get_boto3_session
from dbt.adapters.athena.connections_legacy import AthenaConnectionManager as PyAthenaConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.sql import SQLConnectionManager


Cell: TypeAlias = Union[
    None, str, int, float, bool, date, datetime, time, bytes, UUID, IPv4Address, IPv6Address, Any
]
Row: TypeAlias = Tuple[Cell, ...]
ColumnMetadata: TypeAlias = Tuple[str, str, None, None, None, None, None]


@dataclass
class AthenaAdapterResponse(AdapterResponse):
    data_scanned_in_bytes: Optional[int] = None


@dataclass
class AthenaCredentials(Credentials):
    region_name: str
    s3_staging_dir: Optional[str] = None
    endpoint_url: Optional[str] = None
    work_group: Optional[str] = None
    skip_workgroup_check: bool = False
    aws_profile_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    poll_interval: float = 1.0
    debug_query_state: bool = False
    _ALIASES = {"catalog": "database"}
    num_retries: int = 5
    num_boto3_retries: Optional[int] = None
    num_iceberg_retries: int = 3
    s3_data_dir: Optional[str] = None
    s3_data_naming: str = "schema_table_unique"
    spark_work_group: Optional[str] = None
    s3_tmp_table_dir: Optional[str] = None
    # Unfortunately we can not just use dict, must be Dict because we'll get the following error:
    # Credentials in profile "athena", target "athena" invalid: Unable to create schema for 'dict'
    seed_s3_upload_args: Optional[Dict[str, Any]] = None
    lf_tags_database: Optional[Dict[str, str]] = None
    connection_manager: str = "api"

    @property
    def type(self) -> str:
        return "athena"

    @property
    def unique_field(self) -> str:
        return f"athena-{md5(self.s3_staging_dir)}"

    @property
    def effective_num_retries(self) -> int:
        return self.num_boto3_retries or self.num_retries

    def _connection_keys(self) -> Tuple[str, ...]:
        return (
            "s3_staging_dir",
            "work_group",
            "skip_workgroup_check",
            "region_name",
            "database",
            "schema",
            "poll_interval",
            "aws_profile_name",
            "aws_access_key_id",
            "endpoint_url",
            "s3_data_dir",
            "s3_data_naming",
            "s3_tmp_table_dir",
            "debug_query_state",
            "seed_s3_upload_args",
            "lf_tags_database",
            "spark_work_group",
        )


class AthenaError(Exception):
    pass


class AthenaQueryCancelledError(AthenaError):
    pass


class AthenaQueryFailedError(AthenaError):
    CATEGORY_SYSTEM = 1
    CATEGORY_USER = 2
    CATEGORY_OTHER = 3

    TYPE_ICEBERG_ERROR = 233

    error_category: int
    error_type: int
    retryable: bool

    def __init__(self, athena_error: Dict[str, Any]) -> None:
        super().__init__(athena_error["ErrorMessage"])
        self.error_category = athena_error["ErrorCategory"]
        self.error_type = athena_error["ErrorType"]
        self.retryable = athena_error["Retryable"]


class AthenaParameterFormatter:
    def format(self, operation: str, parameters: Optional[List[str]] = None) -> str:
        if operation is None or not operation.strip():
            raise ValueError("Query is none or empty.")
        elif not (parameters is None or isinstance(parameters, list)):
            raise ValueError("Parameters must be a list.")

        if operation.upper().startswith(("VACUUM", "OPTIMIZE")):
            operation = operation.replace('"', "")
        elif not operation.upper().startswith(("SELECT", "WITH", "INSERT")):
            operation = operation.replace("\n\n    ", "\n")

        if parameters is not None:
            kwargs = [self._format_value(v) for v in parameters]
            operation = (operation % tuple(kwargs)).strip()

        return operation.strip()

    def _format_value(self, value: Any, force_str: bool = False) -> Union[str, int, float]:
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return str(value).upper()
        elif isinstance(value, int) and force_str:
            return str(value)
        elif isinstance(value, int):
            return value
        elif isinstance(value, float) and force_str:
            return f"{value:f}"
        elif isinstance(value, float):
            return value
        elif isinstance(value, Decimal) and value == int(value):
            return f"DECIMAL '{value}'"
        elif isinstance(value, Decimal):
            return f"DECIMAL '{value:f}'"
        elif isinstance(value, datetime):
            return f"""TIMESTAMP '{value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""
        elif isinstance(value, date):
            return f"DATE '{value:%Y-%m-%d}'"
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, list) or isinstance(value, set) or isinstance(value, tuple):
            formatted = [str(self._format_value(v, True)) for v in value]
            return f'({", ".join(formatted)})'
        else:
            raise TypeError(f"No parameter formatter found for type {type(value)}.")


API_REQUEST_ERROR_NAMES = [
    "TooManyRequestsException",
    "ThrottlingException",
    "InternalServerException",
]


class AthenaCursor:
    query: Optional[str]
    state: Optional[str]
    data_scanned_in_bytes: int

    STATE_SUCCEEDED: str = "SUCCEEDED"
    STATE_CANCELLED: str = "CANCELLED"
    STATE_FAILED: str = "FAILED"

    def __init__(
        self,
        athena_client: Any,
        credentials: AthenaCredentials,
        formatter: AthenaParameterFormatter = AthenaParameterFormatter(),
        poll_delay: Callable[[float], None] = sleep,
        retry_interval_multiplier: int = 1,
    ) -> None:
        self._client = athena_client
        self._credentials = credentials
        self._poll_delay = poll_delay
        self._formatter = formatter
        self._with_throttling_retries = Retrying(
            retry=retry_if_exception(
                lambda e: any(error_name in str(e) for error_name in API_REQUEST_ERROR_NAMES)
            ),
            stop=stop_after_attempt(self._credentials.num_retries + 1),
            wait=wait_random_exponential(max=100, multiplier=retry_interval_multiplier),
            reraise=True,
        )
        self._with_iceberg_retries = Retrying(
            retry=retry_if_exception(
                lambda e: (
                    isinstance(e, AthenaQueryFailedError)
                    and e.error_type == AthenaQueryFailedError.TYPE_ICEBERG_ERROR
                    and "ICEBERG_COMMIT_ERROR" in str(e)
                )
            ),
            stop=stop_after_attempt(self._credentials.num_iceberg_retries + 1),
            wait=wait_random_exponential(max=100, multiplier=retry_interval_multiplier),
            reraise=True,
        )
        self._reset()

    def _reset(self) -> None:
        self.query = None
        self.state = None
        self.data_scanned_in_bytes = 0
        self._update_count: Optional[int] = None
        self._column_info: List[ColumnMetadata] = []
        self._query_execution_id: Optional[str] = None
        self._result_set: Optional[AthenaResultSet] = None

    def execute(
        self,
        operation: str,
        parameters: Optional[List[str]] = None,
    ) -> Self:
        self._reset()
        self.query = self._formatter.format(operation, parameters)
        LOGGER.debug(f"Execute: {self.query}")
        self._with_iceberg_retries(self._run_query)
        return self

    def _run_query(self) -> None:
        self._with_throttling_retries(self._start_execution)
        self._await_completion()

    def _start_execution(self) -> None:
        request = {
            "QueryString": self.query,
            "WorkGroup": self._credentials.work_group,
            "QueryExecutionContext": {
                "Catalog": self._credentials.database,
                "Database": self._credentials.schema,
            },
        }
        if self._credentials.s3_staging_dir is not None:
            request["ResultConfiguration"] = {
                "OutputLocation": self._credentials.s3_staging_dir,
            }
        start_response = self._client.start_query_execution(**request)
        self._query_execution_id = start_response["QueryExecutionId"]
        LOGGER.debug(f"Athena query {self._query_execution_id} started")

    def _await_completion(self) -> None:
        while True:
            self._poll_delay(self._credentials.poll_interval)
            try:
                status_response = self._client.get_query_execution(
                    QueryExecutionId=self._query_execution_id
                )
            except Exception as e:
                error_code = getattr(e, "response", {}).get("Error", {}).get("Code", None)
                if error_code in API_REQUEST_ERROR_NAMES:
                    LOGGER.warning(
                        f"Athena query {self._query_execution_id} got error while polling status, will retry: {e}"
                    )
                    continue
                else:
                    raise e
            status = status_response["QueryExecution"]["Status"]
            self.state = status["State"]
            LOGGER.debug(f"Athena query {self._query_execution_id} has state {self.state}")
            if self.state == AthenaCursor.STATE_SUCCEEDED:
                statistics = status_response["QueryExecution"]["Statistics"]
                self.data_scanned_in_bytes = statistics["DataScannedInBytes"]
                if self._query_execution_id:
                    plain_text = self._is_plain_text_result(status_response)
                    self._result_set = AthenaResultSet(
                        self._client, self._query_execution_id, plain_text
                    )
                    break
                else:
                    raise AthenaError("Could not create result set, query execution ID lost")
            elif self.state == AthenaCursor.STATE_FAILED:
                raise AthenaQueryFailedError(status["AthenaError"])
            elif self.state == self.state == AthenaCursor.STATE_CANCELLED:
                raise AthenaQueryCancelledError(status["StateChangeReason"])

    def _is_plain_text_result(self, status_response: Dict[str, Any]) -> bool:
        query_execution = status_response["QueryExecution"]
        statement_type = query_execution["StatementType"]
        statement_subtype = query_execution["SubstatementType"]
        return (
            statement_type == "DDL"
            or statement_type == "UTILITY"
            or statement_subtype == "EXPLAIN"
        )

    @property
    def description(self) -> Optional[List[ColumnMetadata]]:
        if self._result_set:
            return self._result_set.column_info
        else:
            return None

    @property
    def rowcount(self) -> int:
        if self._result_set:
            return self._result_set.update_count
        else:
            return -1

    def fetchone(self) -> Optional[Row]:
        rows = self.fetchmany(1)
        return rows[0]

    def fetchmany(self, limit: Optional[int] = None) -> List[Row]:
        if self._result_set:
            if limit is None:
                return list(self._result_set)
            else:
                rows = []
                for row in self._result_set:
                    rows.append(row)
                    if limit is not None and len(rows) == limit:
                        break
                return rows
        else:
            raise AthenaError(f"Cannot fetch from a cursor with state {self.state}")

    def fetchall(self) -> List[Row]:
        return self.fetchmany()


class AthenaResultSet(Iterator[Row]):
    def __init__(
        self,
        athena_client: Any,
        query_execution_id: str,
        is_plain_text: bool,
        limit: Optional[int] = None,
    ) -> None:
        self._client = athena_client
        self._query_execution_id = query_execution_id
        self._limit = limit
        self._headers_skipped = False
        self._is_plain_text = is_plain_text
        self._rows: Deque[Row] = deque()
        self._next_token: Optional[str] = None
        self._column_info: Optional[List[ColumnMetadata]] = None
        self._update_count: Optional[int] = None
        self._items = 0

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Row:
        if not self._rows and (self._next_token is not None or not self._headers_skipped):
            self._load_page()
        if self._rows and (self._limit is None or self._items < self._limit):
            self._items += 1
            return self._rows.popleft()
        else:
            raise StopIteration

    @property
    def column_info(self) -> List[ColumnMetadata]:
        if not self._column_info:
            self._load_page()
        if self._column_info:
            return self._column_info
        else:
            return []

    @property
    def update_count(self) -> int:
        if self._update_count is None:
            self._load_page()
        if self._update_count:
            return self._update_count
        else:
            return -1

    def _load_page(self) -> None:
        request: Dict[str, Any] = {"QueryExecutionId": self._query_execution_id}
        if self._next_token:
            request["NextToken"] = self._next_token
        results_response = self._client.get_query_results(**request)
        self._next_token = results_response.get("NextToken", None)
        page_rows = results_response["ResultSet"]["Rows"]
        if not self._headers_skipped:
            if not self._is_plain_text:
                page_rows = page_rows[1:]
            self._column_info = self._convert_column_info(
                results_response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            )
            self._update_count = results_response.get("UpdateCount", -1)
            self._headers_skipped = True
        self._rows += [self._convert_row(row) for row in page_rows]

    def _convert_column_info(self, column_info: List[Dict[str, str]]) -> List[ColumnMetadata]:
        return [(info["Name"], info["Type"], None, None, None, None, None) for info in column_info]

    def _convert_row(self, row: Dict[str, List[Dict[str, str]]]) -> Row:
        if self._column_info:
            return tuple(
                self._convert_type(self._column_info[index], datum)
                for index, datum in enumerate(row["Data"])
            )
        else:
            raise AthenaError("No column info found")

    def _convert_type(self, column_info: ColumnMetadata, datum: Dict[str, str]) -> Cell:
        if "VarCharValue" not in datum:
            return None
        value = datum["VarCharValue"]
        type = column_info[1]
        if (
            type == "int"
            or type == "integer"
            or type == "bigint"
            or type == "tinyint"
            or type == "smallint"
        ):
            return int(value)
        elif type == "float" or type == "double":
            return float(value)
        elif type == "boolean":
            return value.lower() != "false"
        elif type == "date":
            return datetime.strptime(value, "%Y-%m-%d").date()
        elif type == "timestamp":
            return self._parse_timestamp(value)
        elif type == "timestamp with time zone":
            return self._parse_timestamp_with_time_zone(value)
        elif type == "time":
            return self._parse_time(value)
        elif type == "time with time zone":
            return self._parse_time_with_time_zone(value)
        elif type == "varbinary":
            return bytes.fromhex(value)
        elif type == "json":
            return json.loads(value)
        elif type == "uuid":
            return UUID(value)
        elif type == "ipaddress":
            return ip_address(value)
        else:
            return value

    TIMESTAMP_FORMATS = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]

    def _parse_timestamp(self, value: str) -> datetime:
        for fmt in self.__class__.TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                pass
        raise ValueError(f'Could not parse timestamp "{value}"')

    def _parse_timestamp_with_time_zone(self, value: str) -> datetime:
        try:
            timestamp_value, tz_value = value.rsplit(" ", 1)
            timestamp = self._parse_timestamp(timestamp_value)
            tz = self._parse_time_zone(tz_value)
            return timestamp.replace(tzinfo=tz)
        except ValueError:
            raise ValueError(f'Could not parse timestamp with time zone "{value}"')

    def _parse_time_zone(self, value: str) -> tzinfo:
        if value == "UTC":
            return timezone.utc
        else:
            try:
                return datetime.strptime(value, "%z").tzinfo
            except ValueError:
                try:
                    return ZoneInfo(value)
                except ZoneInfoNotFoundError:
                    raise ValueError(f'Could not parse time zone "{value}"')

    TIME_FORMATS = [
        "%H:%M:%S.%f",
        "%H:%M:%S",
    ]

    def _parse_time(self, value: str) -> time:
        for fmt in self.__class__.TIME_FORMATS:
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                pass
        raise ValueError(f'Could not parse time "{value}"')

    def _parse_time_with_time_zone(self, value: str) -> time:
        try:
            parts = re.split(r"(?=[- +])", value)
            time = self._parse_time(parts[0])
            tz = self._parse_time_zone(parts[-1].strip())
            return time.replace(tzinfo=tz)
        except ValueError:
            raise ValueError(f'Could not parse time with time zone "{value}"')

class AthenaConnection(Connection):
    credentials: AthenaCredentials
    session: BotoSession
    region_name: str

    def __init__(
        self,
        credentials: AthenaCredentials,
        boto_session_factory: Callable[[Connection], BotoSession] = get_boto3_session,
    ) -> None:
        self.credentials = credentials
        self.region_name = self.credentials.region_name
        self.session = boto_session_factory(self)
        self._client = None

    def connect(self, boto_config_factory: Callable[..., BotoConfig] = get_boto3_config) -> Self:
        boto_config = boto_config_factory(num_retries=self.credentials.effective_num_retries)
        self._client = self.session.client(
            "athena", region_name=self.credentials.region_name, config=boto_config
        )
        return self

    def cursor(self) -> AthenaCursor:
        return AthenaCursor(self._client, self.credentials)


class AthenaConnectionManager(SQLConnectionManager):
    TYPE = "athena"

    def set_query_header(self, query_header_context: Dict[str, Any]) -> None:
        self.query_header = AthenaMacroQueryStringSetter(self.profile, query_header_context)

    @classmethod
    def data_type_code_to_name(cls, type_code: int | str) -> str:
        """
        Get the string representation of the data type from the Athena metadata. Dbt performs a
        query to retrieve the types of the columns in the SQL query. Then these types are compared
        to the types in the contract config, simplified because they need to match what is returned
        by Athena metadata (we are only interested in the broader type, without subtypes nor granularity).
        """
        return str(type_code).split("(")[0].split("<")[0].upper()

    @contextmanager  # type: ignore
    def exception_handler(self, sql: str) -> ContextManager:  # type: ignore
        try:
            yield
        except Exception as e:
            LOGGER.debug(f"Error running SQL: {sql}")
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            LOGGER.debug("Connection is already open, skipping open.")
            return connection

        try:
            credentials = cast(AthenaCredentials, connection.credentials)
            if credentials.connection_manager is not None and credentials.connection_manager.lower() == "pyathena":
                return PyAthenaConnectionManager.open(connection)
            else:
                connection.handle = AthenaConnection(credentials).connect()
                connection.state = ConnectionState.OPEN
        except ConnectionError as exc:
            raise exc
        except Exception as exc:
            LOGGER.exception(
                f"Got an error when attempting to open a Athena connection due to {exc}"
            )
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise ConnectionError(str(exc))

        return connection

    @classmethod
    def get_response(cls, cursor: AthenaCursor) -> AthenaAdapterResponse:
        code = "OK" if cursor.state == AthenaCursor.STATE_SUCCEEDED else "ERROR"
        rowcount, data_scanned_in_bytes = cls.process_query_stats(cursor)
        return AthenaAdapterResponse(
            _message=f"{code} {rowcount}",
            rows_affected=rowcount,
            code=code,
            data_scanned_in_bytes=data_scanned_in_bytes,
        )

    @staticmethod
    def process_query_stats(cursor: AthenaCursor) -> Tuple[int, int]:
        """
        Helper function to parse query statistics from SELECT statements.
        The function looks for all statements that contains rowcount or data_scanned_in_bytes,
        then strip the SELECT statements, and pick the value between curly brackets.
        """
        if cursor.query is not None and all(
            map(cursor.query.__contains__, ["rowcount", "data_scanned_in_bytes"])
        ):
            try:
                query_split = cursor.query.lower().split("select")[-1]
                # query statistics are in the format {"rowcount":1, "data_scanned_in_bytes": 3}
                # the following statement extract the content between { and }
                query_stats = re.search("{(.*)}", query_split)
                if query_stats:
                    stats = json.loads("{" + query_stats.group(1) + "}")
                    return stats.get("rowcount", -1), stats.get("data_scanned_in_bytes", 0)
            except Exception as err:
                LOGGER.debug(f"There was an error parsing query stats {err}")
                return -1, 0
        return cursor.rowcount, cursor.data_scanned_in_bytes

    def cancel(self, connection: Connection) -> None:
        pass

    def add_begin_query(self) -> None:
        pass

    def add_commit_query(self) -> None:
        pass

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass
