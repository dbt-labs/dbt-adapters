import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
import math
from ipaddress import ip_address
from unittest import mock
from uuid import UUID

import botocore
import pytest

from dbt.adapters.athena.connections import (
    AthenaConnection,
    AthenaCredentials,
    AthenaCursor,
    AthenaQueryCancelledError,
    AthenaQueryFailedError,
)

from .constants import ATHENA_WORKGROUP, AWS_REGION


class TestAthenaConnection:
    @pytest.fixture
    def credentials(self):
        credentials = AthenaCredentials(
            database="my_database",
            schema="my_schema",
            s3_staging_dir="s3://test-bucket/staging-location",
            region_name=AWS_REGION,
            work_group=ATHENA_WORKGROUP,
        )
        return credentials

    @pytest.fixture
    def athena_client(self):
        client = mock.Mock()
        client.start_query_execution = mock.Mock(
            return_value={"QueryExecutionId": "query-execution-id"}
        )
        client.get_query_execution = mock.Mock(
            return_value={
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {"DataScannedInBytes": 123},
                },
            }
        )
        return client

    @pytest.fixture
    def session_factory(self, athena_client):
        session = mock.Mock()
        session.client = mock.Mock(return_value=athena_client)
        return mock.Mock(return_value=session)

    @pytest.fixture
    def config_factory(self):
        config = mock.Mock()
        return mock.Mock(return_value=config)

    def test_connect_creates_athena_client(self, credentials, session_factory, config_factory):
        connection = AthenaConnection(credentials, boto_session_factory=session_factory)
        connection.connect(boto_config_factory=config_factory)
        session_factory.assert_called_once_with(connection)
        session = session_factory()
        session.client.assert_called_once_with(
            "athena", region_name=AWS_REGION, config=config_factory()
        )

    def test_connect_returns_self(self, credentials, session_factory, config_factory):
        connection = AthenaConnection(credentials, boto_session_factory=session_factory)
        connection.connect(boto_config_factory=config_factory)
        session_factory.assert_called_once_with(connection)
        assert connection is connection.connect()

    @pytest.fixture
    def connection(self, credentials, session_factory, config_factory):
        connection = AthenaConnection(credentials, boto_session_factory=session_factory)
        return connection.connect(boto_config_factory=config_factory)

    def test_cursor_returns_cursor(self, connection, athena_client):
        cursor = connection.cursor()
        cursor.execute("SELECT NOW()")
        athena_client.start_query_execution.assert_called_once()


STATE_EVENT_QUEUED = {"QueryExecution": {"Status": {"State": "QUEUED"}}}
STATE_EVENT_RUNNING = {"QueryExecution": {"Status": {"State": "RUNNING"}}}
STATE_EVENT_SUCCEEDED = {
    "QueryExecution": {"Status": {"State": "SUCCEEDED"}, "Statistics": {"DataScannedInBytes": 123}}
}
STATE_EVENT_CANCELLED = {
    "QueryExecution": {
        "Status": {"State": "CANCELLED", "StateChangeReason": "Query was cancelled!"}
    }
}
STATE_EVENT_GENERIC_ERROR = {
    "QueryExecution": {
        "Status": {
            "State": "FAILED",
            "AthenaError": {
                "ErrorMessage": "Query failed!",
                "ErrorCategory": 2,
                "ErrorType": 9999,
                "Retryable": True,
            },
        }
    }
}
STATE_EVENT_ICEBERG_COMMIT_ERROR = {
    "QueryExecution": {
        "Status": {
            "State": "FAILED",
            "AthenaError": {
                "ErrorMessage": "ICEBERG_COMMIT_ERROR: could not commit",
                "ErrorCategory": AthenaQueryFailedError.CATEGORY_USER,
                "ErrorType": AthenaQueryFailedError.TYPE_ICEBERG_ERROR,
                "Retryable": False,
            },
        }
    }
}


class TestAthenaCursor:
    @pytest.fixture
    def athena_client(self):
        client = mock.Mock()
        client.start_query_execution = mock.Mock(return_value={"QueryExecutionId": "1234-abcd"})
        client.get_query_execution = mock.Mock(
            return_value={
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {"DataScannedInBytes": 123},
                },
            }
        )
        client.get_query_results = mock.Mock(
            return_value={
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "dt"}, {"VarCharValue": "str"}]},
                        {"Data": [{"VarCharValue": "2024-01-01"}, {"VarCharValue": "a"}]},
                        {"Data": [{"VarCharValue": "2024-01-02"}, {"VarCharValue": "b"}]},
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "dt", "Type": "date"},
                            {"Name": "str", "Type": "varchar"},
                        ],
                    },
                },
            }
        )
        return client

    @pytest.fixture
    def credentials(self):
        credentials = AthenaCredentials(
            database="my_database",
            schema="my_schema",
            s3_staging_dir="s3://test-bucket/staging-location",
            region_name=AWS_REGION,
            work_group=ATHENA_WORKGROUP,
        )
        return credentials

    @pytest.fixture
    def poll_delay(self):
        return mock.Mock()

    @pytest.fixture
    def formatter(self):
        formatter = mock.Mock()
        formatter.format = lambda *args: args[0]
        return formatter

    @pytest.fixture
    def cursor(self, athena_client, credentials, poll_delay, formatter):
        return AthenaCursor(
            athena_client,
            credentials,
            poll_delay=poll_delay,
            formatter=formatter,
            retry_interval_multiplier=0,
        )

    def test_execute_starts_query_execution(self, cursor, athena_client):
        cursor.execute("SELECT NOW()")
        athena_client.start_query_execution.assert_called_once_with(
            QueryString="SELECT NOW()",
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={
                "OutputLocation": "s3://test-bucket/staging-location",
            },
            QueryExecutionContext={
                "Catalog": "my_database",
                "Database": "my_schema",
            },
        )

    def test_execute_formats_the_query(self, cursor, formatter, athena_client):
        formatter.format = mock.Mock(return_value="SELECT 'hello world' || '!'")
        cursor.execute("SELECT %s", ["hello world"])
        query = athena_client.start_query_execution.call_args_list[0].kwargs["QueryString"]
        formatter.format.assert_called_once_with("SELECT %s", ["hello world"])
        assert query == "SELECT 'hello world' || '!'"

    def test_execute_awaits_success(self, cursor, athena_client):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_SUCCEEDED,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        cursor.execute("SELECT NOW()")
        athena_client.get_query_execution.assert_has_calls(
            [
                mock.call(QueryExecutionId="1234-abcd"),
                mock.call(QueryExecutionId="1234-abcd"),
                mock.call(QueryExecutionId="1234-abcd"),
            ]
        )

    def test_execute_delays_between_polls(self, cursor, athena_client, poll_delay, credentials):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_RUNNING,
            STATE_EVENT_RUNNING,
            STATE_EVENT_RUNNING,
            STATE_EVENT_SUCCEEDED,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        cursor.execute("SELECT NOW()")
        poll_delay.assert_has_calls([mock.call(credentials.poll_interval)] * len(state_sequence))

    def test_data_scanned_in_bytes(self, cursor):
        assert cursor.data_scanned_in_bytes == 0
        cursor.execute("SELECT NOW()")
        assert cursor.data_scanned_in_bytes == 123

    def test_query(self, cursor):
        assert cursor.query is None
        cursor.execute("SELECT NOW()")
        assert cursor.query == "SELECT NOW()"

    def test_rowcount_fetches_the_update_count(self, cursor, athena_client):
        athena_client.get_query_results = mock.Mock(
            return_value={
                "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
                "UpdateCount": 1234,
            }
        )
        cursor.execute("INSERT INTO my_table SELECT * FROM other_table")
        assert cursor.rowcount == 1234
        athena_client.get_query_results.assert_called_once_with(
            QueryExecutionId="1234-abcd", MaxResults=1
        )

    def test_rowcount_does_not_fetch_when_the_update_count_has_already_been_fetched(
        self, cursor, athena_client
    ):
        athena_client.get_query_results = mock.Mock(
            return_value={
                "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
                "UpdateCount": 1234,
            }
        )
        cursor.execute("INSERT INTO my_table SELECT * FROM other_table")
        cursor.fetchall()
        assert cursor.rowcount == 1234
        athena_client.get_query_results.assert_called_once_with(QueryExecutionId="1234-abcd")

    def test_rowcount_returns_negative_when_there_is_no_update_count(self, cursor):
        assert cursor.rowcount == -1
        cursor.execute("SELECT NOW()")
        assert cursor.rowcount == -1

    def test_state_returns_the_final_query_state_when_succeeded(self, cursor, athena_client):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_SUCCEEDED,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        cursor.execute("SELECT NOW()")
        assert cursor.state == AthenaCursor.STATE_SUCCEEDED

    def test_state_returns_the_final_state_when_failed(self, cursor, athena_client):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_GENERIC_ERROR,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        with pytest.raises(AthenaQueryFailedError):
            cursor.execute("SELECT NOW()")
        assert cursor.state == AthenaCursor.STATE_FAILED

    def test_state_returns_the_final_query_state_when_cancelled(self, cursor, athena_client):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_CANCELLED,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        with pytest.raises(AthenaQueryCancelledError):
            cursor.execute("SELECT NOW()")
        assert cursor.state == AthenaCursor.STATE_CANCELLED

    def test_execute_awaits_failure_and_raises(self, cursor, athena_client):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_GENERIC_ERROR,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        with pytest.raises(AthenaQueryFailedError) as e:
            cursor.execute("SELECT NOW()")
        assert str(e.value) == "Query failed!"
        assert e.value.error_category == AthenaQueryFailedError.CATEGORY_USER
        assert e.value.error_type == 9999
        assert e.value.retryable is True

    def test_execute_awaits_cancellation_and_raises(self, cursor, athena_client):
        state_sequence = [
            STATE_EVENT_QUEUED,
            STATE_EVENT_RUNNING,
            STATE_EVENT_CANCELLED,
        ]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        with pytest.raises(AthenaQueryCancelledError) as e:
            cursor.execute("SELECT NOW()")
        assert str(e.value) == "Query was cancelled!"

    def test_execute_retries_iceberg_commit_errors(self, cursor, athena_client, credentials):
        state_sequence = [STATE_EVENT_QUEUED]
        state_sequence += [
            STATE_EVENT_RUNNING,
            STATE_EVENT_ICEBERG_COMMIT_ERROR,
        ] * credentials.num_iceberg_retries
        state_sequence += [STATE_EVENT_SUCCEEDED]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        cursor.execute("INSERT INTO time SELECT NOW()")
        assert (
            athena_client.start_query_execution.call_count == credentials.num_iceberg_retries + 1
        )

    def test_execute_fails_after_too_many_iceberg_commit_errors(
        self, cursor, athena_client, credentials
    ):
        state_sequence = [STATE_EVENT_QUEUED]
        state_sequence += [STATE_EVENT_RUNNING, STATE_EVENT_ICEBERG_COMMIT_ERROR] * (
            credentials.num_iceberg_retries + 1
        )
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        with pytest.raises(AthenaQueryFailedError) as e:
            cursor.execute("INSERT INTO time SELECT NOW()")
        assert str(e.value) == "ICEBERG_COMMIT_ERROR: could not commit"

    API_REQUEST_ERROR_NAMES = [
        "TooManyRequestsException",
        "ThrottlingException",
        "InternalServerException",
    ]

    @pytest.mark.parametrize("exception_name", API_REQUEST_ERROR_NAMES)
    def test_execute_retries_on_api_request_errors_from_start_query_execution(
        self, cursor, athena_client, credentials, exception_name
    ):
        client_error = botocore.exceptions.ClientError(
            {"Error": {"Code": exception_name}}, "StartQueryExecution"
        )
        result_sequence = [client_error] * credentials.num_retries
        result_sequence += [{"QueryExecutionId": "1234-abcd"}]
        athena_client.start_query_execution = mock.Mock(side_effect=result_sequence)
        cursor.execute("SELECT NOW()")
        assert athena_client.start_query_execution.call_count == credentials.num_retries + 1

    @pytest.mark.parametrize("exception_name", API_REQUEST_ERROR_NAMES)
    def test_execute_fails_on_too_many_api_request_errors_from_start_query_execution(
        self, cursor, athena_client, credentials, exception_name
    ):
        client_error = botocore.exceptions.ClientError(
            {"Error": {"Code": exception_name}}, "StartQueryExecution"
        )
        result_sequence = [client_error] * (credentials.num_retries + 1)
        athena_client.start_query_execution = mock.Mock(side_effect=result_sequence)
        with pytest.raises(Exception) as e:
            cursor.execute("SELECT NOW()")
        assert exception_name in str(e.value)

    @pytest.mark.parametrize("exception_name", API_REQUEST_ERROR_NAMES)
    def test_execute_ignores_api_request_errors_from_get_query_execution(
        self, cursor, athena_client, credentials, exception_name
    ):
        client_error = botocore.exceptions.ClientError(
            {"Error": {"Code": exception_name}}, "GetQueryExecution"
        )
        state_sequence = [STATE_EVENT_QUEUED]
        state_sequence += [client_error] * 3
        state_sequence += [STATE_EVENT_RUNNING]
        state_sequence += [client_error] * 2
        state_sequence += [STATE_EVENT_SUCCEEDED]
        athena_client.get_query_execution = mock.Mock(side_effect=state_sequence)
        cursor.execute("SELECT NOW()")
        assert athena_client.get_query_execution.call_count == len(state_sequence)

    def test_execute_fails_on_invalid_request_error(self, cursor, athena_client):
        client_error = botocore.exceptions.ClientError(
            {"Error": {"Code": "InvalidRequestException"}}, "StartQueryExecution"
        )
        athena_client.start_query_execution = mock.Mock(side_effect=client_error)
        with pytest.raises(Exception) as e:
            cursor.execute("SELECT NOW()")
        assert "InvalidRequestException" in str(e.value)

    def test_description_loads_column_metadata(self, cursor, athena_client):
        cursor.execute("SELECT * FROM table")
        assert cursor.description == [("dt", "date"), ("str", "varchar")]
        athena_client.get_query_results.assert_called_once_with(
            QueryExecutionId="1234-abcd", MaxResults=1
        )

    def test_description_returns_none_on_failure(self, cursor, athena_client):
        athena_client.get_query_execution = mock.Mock(return_value=STATE_EVENT_GENERIC_ERROR)
        with pytest.raises(AthenaQueryFailedError):
            cursor.execute("SELECT * FROM table")
        assert cursor.description is None

    def test_fetchone_loads_one_row_and_skips_the_header_row(self, cursor, athena_client):
        cursor.execute("SELECT * FROM table")
        row = cursor.fetchone()
        assert row == (datetime.date(2024, 1, 1), "a")

    def _create_page(self, athena_client, column_info, rows, include_header=True, next_token=None):
        formatted_rows = [
            {"Data": [({"VarCharValue": v} if v is not None else {}) for v in row]} for row in rows
        ]
        if include_header:
            header = [{"Data": [{"VarCharValue": name} for (name, _) in column_info]}]
            formatted_rows.insert(0, header)
        response = {
            "ResultSet": {
                "Rows": formatted_rows,
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": name, "Type": type} for (name, type) in column_info],
                },
            },
        }
        if next_token is not None:
            response["NextToken"] = next_token
        return response

    def test_fetchmany_loads_one_page_and_skips_the_header_row(self, cursor, athena_client):
        data = [[f"2024-01-{(n + 1):02d}", f"{n}"] for n in range(10)]
        page = self._create_page(athena_client, [("dt", "date"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchmany(10))
        assert len(rows) == 10
        assert rows[0] == (datetime.date(2024, 1, 1), 0)
        assert rows[1] == (datetime.date(2024, 1, 2), 1)
        assert rows[-1] == (datetime.date(2024, 1, 10), 9)

    def test_fetchmany_loads_only_as_many_rows_as_needed(self, cursor, athena_client):
        data = [[f"2024-01-{(n + 1):02d}", f"{n}"] for n in range(10)]
        page = self._create_page(athena_client, [("dt", "date"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        cursor.fetchmany(10)
        athena_client.get_query_results.assert_called_once_with(
            QueryExecutionId="1234-abcd", MaxResults=11
        )

    def test_fetchmany_loads_full_pages_when_the_limit_is_larger_than_a_page(
        self, cursor, athena_client
    ):
        data = [[f"2024-01-{(n + 1):02d}", f"{n}"] for n in range(10)]
        page = self._create_page(athena_client, [("dt", "date"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        cursor.fetchmany(1111)
        athena_client.get_query_results.assert_called_once_with(QueryExecutionId="1234-abcd")

    def test_fetchmany_returns_fewer_rows_than_requested_when_there_are_no_more_rows(
        self, cursor, athena_client
    ):
        data = [[f"2024-01-{(n + 1):02d}", f"{n}"] for n in range(5)]
        page = self._create_page(athena_client, [("dt", "date"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchmany(10))
        assert len(rows) == 5
        assert rows[0] == (datetime.date(2024, 1, 1), 0)
        assert rows[-1] == (datetime.date(2024, 1, 5), 4)

    def test_fetchmany_returns_as_many_rows_as_requested_when_there_are_more_rows(
        self, cursor, athena_client
    ):
        data = [[f"2024-01-{(n + 1):02d}", f"{n}"] for n in range(10)]
        page = self._create_page(athena_client, [("dt", "date"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchmany(5))
        assert len(rows) == 5
        assert rows[0] == (datetime.date(2024, 1, 1), 0)
        assert rows[-1] == (datetime.date(2024, 1, 5), 4)

    def test_fetchmany_loads_more_pages_as_needed(self, cursor, athena_client):
        data = [[f"{n}"] for n in range(3999)]
        page1 = self._create_page(
            athena_client, [("n", "int")], data[0:999], include_header=True, next_token="p2"
        )
        page2 = self._create_page(
            athena_client, [("n", "int")], data[999:1999], include_header=False, next_token="p3"
        )
        page3 = self._create_page(
            athena_client, [("n", "int")], data[1999:2999], include_header=False, next_token="p4"
        )
        page4 = self._create_page(
            athena_client, [("n", "int")], data[2999:], include_header=False, next_token=None
        )
        athena_client.get_query_results = mock.Mock(side_effect=[page1, page2, page3, page4])
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchmany(3333))
        assert len(rows) == 3333
        assert rows[0] == (0,)
        assert rows[-1] == (3332,)
        athena_client.get_query_results.assert_has_calls(
            [
                mock.call(QueryExecutionId="1234-abcd"),
                mock.call(QueryExecutionId="1234-abcd", NextToken="p2"),
                mock.call(QueryExecutionId="1234-abcd", NextToken="p3"),
                mock.call(QueryExecutionId="1234-abcd", NextToken="p4"),
            ]
        )

    def test_fetchall_loads_one_page_and_skips_the_header_row(self, cursor, athena_client):
        data = [[f"2024-01-{(n + 1):02d}", f"{n}"] for n in range(10)]
        page = self._create_page(athena_client, [("dt", "date"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert len(rows) == 10
        assert rows[0] == (datetime.date(2024, 1, 1), 0)
        assert rows[1] == (datetime.date(2024, 1, 2), 1)
        assert rows[9] == (datetime.date(2024, 1, 10), 9)

    def test_fetchall_loads_more_pages_as_needed(self, cursor, athena_client):
        data = [[f"{n}"] for n in range(3999)]
        page1 = self._create_page(
            athena_client, [("n", "int")], data[0:999], include_header=True, next_token="p2"
        )
        page2 = self._create_page(
            athena_client, [("n", "int")], data[999:1999], include_header=False, next_token="p3"
        )
        page3 = self._create_page(
            athena_client, [("n", "int")], data[1999:2999], include_header=False, next_token="p4"
        )
        page4 = self._create_page(
            athena_client, [("n", "int")], data[2999:], include_header=False, next_token=None
        )
        athena_client.get_query_results = mock.Mock(side_effect=[page1, page2, page3, page4])
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert len(rows) == 3999
        assert rows[0] == (0,)
        assert rows[-1] == (3998,)
        athena_client.get_query_results.assert_has_calls(
            [
                mock.call(QueryExecutionId="1234-abcd"),
                mock.call(QueryExecutionId="1234-abcd", NextToken="p2"),
                mock.call(QueryExecutionId="1234-abcd", NextToken="p3"),
                mock.call(QueryExecutionId="1234-abcd", NextToken="p4"),
            ]
        )

    def test_fetch_converts_tinyints(self, cursor, athena_client):
        data = [
            ["-1"],
            ["1"],
            ["127"],
            ["-128"],
        ]
        page = self._create_page(athena_client, [("n", "tinyint")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            (-1,),
            (1,),
            (127,),
            (-128,),
        ]

    def test_fetch_converts_smallints(self, cursor, athena_client):
        data = [
            ["-1"],
            ["1"],
            ["32767"],
            ["-32768"],
        ]
        page = self._create_page(athena_client, [("n", "smallint")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            (-1,),
            (1,),
            (32767,),
            (-32768,),
        ]

    def test_fetch_converts_integers(self, cursor, athena_client):
        data = [
            ["-1"] * 2,
            ["1"] * 2,
            ["1234567"] * 2,
            ["2147483647"] * 2,
        ]
        page = self._create_page(athena_client, [("n", "integer"), ("n", "int")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            (-1, -1),
            (1, 1),
            (1234567, 1234567),
            (2147483647, 2147483647),
        ]

    def test_fetch_converts_big_integers(self, cursor, athena_client):
        data = [
            ["-1"],
            ["1"],
            ["1234567"],
            ["4294967296"],
            ["9223372036854775807"],
        ]
        page = self._create_page(athena_client, [("n", "bigint")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            (-1,),
            (1,),
            (1234567,),
            (4294967296,),
            (9223372036854775807,),
        ]

    def test_fetch_converts_floats(self, cursor, athena_client):
        data = [
            ["3.14", "3.141592653589793"],
            ["0.0", "-0.0"],
            ["-Infinity", "Infinity"],
            ["NaN", "NaN"],
            ["1234.5678", "123456.7890"],
        ]
        page = self._create_page(athena_client, [("n1", "float"), ("n2", "double")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (3.14, 3.141592653589793)
        assert rows[1] == (0.0, -0.0)
        assert math.isinf(rows[2][0])
        assert math.isinf(rows[2][1])
        assert math.isnan(rows[3][0])
        assert math.isnan(rows[3][1])
        assert rows[4] == (1234.5678, 123456.7890)

    def test_fetch_converts_booleans(self, cursor, athena_client):
        data = [
            ["true"],
            ["TRUE"],
            ["false"],
            ["FALSE"],
        ]
        page = self._create_page(athena_client, [("b", "boolean")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [(True,), (True,), (False,), (False,)]

    def test_fetch_converts_dates(self, cursor, athena_client):
        data = [
            ["2024-01-01"],
            ["2024-02-29"],
            ["2024-07-06"],
            ["2024-12-31"],
        ]
        page = self._create_page(athena_client, [("dt", "date")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (datetime.date(2024, 1, 1),)
        assert rows[1] == (datetime.date(2024, 2, 29),)
        assert rows[2] == (datetime.date(2024, 7, 6),)
        assert rows[3] == (datetime.date(2024, 12, 31),)

    def test_fetch_converts_timestamps(self, cursor, athena_client):
        data = [
            ["2024-01-01 10:11:12.013"],
            ["2024-02-29 23:59:59.999"],
            ["2024-07-06 00:00:00"],
            ["2024-12-31 00:00:00.001"],
        ]
        page = self._create_page(athena_client, [("ts", "timestamp")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (datetime.datetime(2024, 1, 1, 10, 11, 12, 13000),)
        assert rows[1] == (datetime.datetime(2024, 2, 29, 23, 59, 59, 999000),)
        assert rows[2] == (datetime.datetime(2024, 7, 6, 0, 0, 0, 0),)
        assert rows[3] == (datetime.datetime(2024, 12, 31, 0, 0, 0, 1000),)

    def test_fetch_converts_timestamps_with_time_zone(self, cursor, athena_client):
        data = [
            ["2024-01-01 10:11:12.013 UTC"],
            ["2024-02-29 23:59:59 UTC"],
            ["2024-01-01 10:11:12.013 America/New_York"],
            ["2024-01-01 10:11:12.013 -00:30"],
        ]
        page = self._create_page(athena_client, [("ts", "timestamp with time zone")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (datetime.datetime(2024, 1, 1, 10, 11, 12, 13000, tzinfo=timezone.utc),)
        assert rows[1] == (datetime.datetime(2024, 2, 29, 23, 59, 59, tzinfo=timezone.utc),)
        assert rows[2] == (datetime.datetime(2024, 1, 1, 10, 11, 12, 13000, tzinfo=ZoneInfo("America/New_York")),)
        assert rows[3] == (datetime.datetime(2024, 1, 1, 10, 11, 12, 13000, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30))),)

    def test_fetch_converts_times(self, cursor, athena_client):
        data = [
            ["10:11:12.013"],
            ["23:59:59.999"],
            ["00:00:00"],
            ["00:00:00.001"],
        ]
        page = self._create_page(athena_client, [("t", "time")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (datetime.time(10, 11, 12, 13000),)
        assert rows[1] == (datetime.time(23, 59, 59, 999000),)
        assert rows[2] == (datetime.time(0, 0, 0, 0),)
        assert rows[3] == (datetime.time(0, 0, 0, 1000),)

    def test_fetch_converts_times_with_time_zone(self, cursor, athena_client):
        data = [
            ["10:11:12.013 UTC"],
            ["23:59:59 UTC"],
            ["00:00:00 America/New_York"],
            ["00:00:00.001-00:30"],
            ["00:00:00.001+00:30"],
            ["00:00:00.001 -00:30"],
        ]
        page = self._create_page(athena_client, [("t", "time with time zone")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (datetime.time(10, 11, 12, 13000, tzinfo=timezone.utc),)
        assert rows[1] == (datetime.time(23, 59, 59, tzinfo=timezone.utc),)
        assert rows[2] == (datetime.time(0, 0, 0, tzinfo=ZoneInfo("America/New_York")),)
        assert rows[3] == (datetime.time(0, 0, 0, 1000, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30))),)
        assert rows[4] == (datetime.time(0, 0, 0, 1000, tzinfo=datetime.timezone(datetime.timedelta(minutes=30))),)
        assert rows[5] == (datetime.time(0, 0, 0, 1000, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30))),)

    def test_fetch_converts_varbinary(self, cursor, athena_client):
        data = [
            ["68 65 6c 6c 6f 20 77 6f 72 6c 64"],
            [""],
            ["00"],
        ]
        page = self._create_page(athena_client, [("vb", "varbinary")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows[0] == (b"hello world",)
        assert rows[1] == (b"",)
        assert rows[2] == (b"\x00",)

    def test_fetch_converts_json(self, cursor, athena_client):
        data = [
            ["[1, 2, 3]"],
            ['"hello world"'],
            ['{"a": 1, "b": {"c": [3]}}'],
        ]
        page = self._create_page(athena_client, [("j", "json")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            ([1, 2, 3],),
            ("hello world",),
            ({"a": 1, "b": {"c": [3]}},),
        ]

    def test_fetch_converts_uuids(self, cursor, athena_client):
        data = [
            ["e50e499b-982f-4cbe-9f50-e11b1c83572e"],
            ["f3ee5fc5-69bb-4413-8f17-6352f259a881"],
        ]
        page = self._create_page(athena_client, [("u", "uuid")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            (UUID("e50e499b-982f-4cbe-9f50-e11b1c83572e"),),
            (UUID("f3ee5fc5-69bb-4413-8f17-6352f259a881"),),
        ]

    def test_fetch_converts_ipaddresses(self, cursor, athena_client):
        data = [
            ["1.1.1.1"],
            ["2001:db8::1"],
            ["192.168.0.0"],
        ]
        page = self._create_page(athena_client, [("ip", "ipaddress")], data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [
            (ip_address("1.1.1.1"),),
            (ip_address("2001:db8::1"),),
            (ip_address("192.168.0.0"),),
        ]

    def test_fetch_converts_null(self, cursor, athena_client):
        types = [
            "varchar",
            "char",
            "int",
            "bigint",
            "float",
            "double",
            "boolean",
            "date",
            "timestamp",
            "time",
        ]
        column_info = [(f"c{n}", type) for n, type in enumerate(types)]
        data = [[None for _ in types]]
        page = self._create_page(athena_client, column_info, data)
        athena_client.get_query_results = mock.Mock(return_value=page)
        cursor.execute("SELECT * FROM table")
        rows = list(cursor.fetchall())
        assert rows == [(None,) * len(types)]
