from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import get_context
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution

from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.connections import AthenaAdapterResponse, AthenaCursor


class TestAthenaConnectionManager:
    @pytest.mark.parametrize(
        ("state", "result"),
        (
            pytest.param(AthenaQueryExecution.STATE_SUCCEEDED, "OK"),
            pytest.param(AthenaQueryExecution.STATE_CANCELLED, "ERROR"),
        ),
    )
    def test_get_response(self, state, result):
        cursor = mock.MagicMock()
        cursor.rowcount = 1
        cursor.state = state
        cursor.data_scanned_in_bytes = 123
        cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
        response = cm.get_response(cursor)
        assert isinstance(response, AthenaAdapterResponse)
        assert response.code == result
        assert response.rows_affected == 1
        assert response.data_scanned_in_bytes == 123

    def test_data_type_code_to_name(self):
        cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
        assert cm.data_type_code_to_name("array<string>") == "ARRAY"
        assert cm.data_type_code_to_name("map<int, boolean>") == "MAP"
        assert cm.data_type_code_to_name("DECIMAL(3, 7)") == "DECIMAL"


class TestAthenaCursorRetry:
    def _make_cursor(self, num_retries=1, num_iceberg_retries=3):
        """Create a minimal AthenaCursor with mocked internals for retry testing."""
        cursor = AthenaCursor.__new__(AthenaCursor)
        retry_config = MagicMock()
        retry_config.attempt = num_retries + 1
        retry_config.max_delay = 0
        retry_config.exponential_base = 1
        cursor._retry_config = retry_config
        cursor._connection = MagicMock()
        cursor._connection.cursor_kwargs = {"num_iceberg_retries": num_iceberg_retries}
        cursor._executor = ThreadPoolExecutor()
        return cursor

    def _make_failed_result(self, reason):
        result = MagicMock()
        result.state = AthenaQueryExecution.STATE_FAILED
        result.state_change_reason = reason
        return result

    def test_iceberg_filesystem_error_is_not_retried(self):
        """ICEBERG_FILESYSTEM_ERROR should not be retried by the outer retry."""
        cursor = self._make_cursor(num_retries=3)
        cursor._execute = MagicMock(return_value="fake-query-id")
        cursor._collect_result_set = MagicMock(
            return_value=self._make_failed_result(
                "ICEBERG_FILESYSTEM_ERROR: Cannot create a table on a non-empty location"
            )
        )

        with pytest.raises(OperationalError, match="ICEBERG_FILESYSTEM_ERROR"):
            cursor.execute("CREATE TABLE AS SELECT 1")

        assert cursor._execute.call_count == 1

    def test_too_many_open_partitions_not_retried_when_catch_enabled(self):
        """TOO_MANY_OPEN_PARTITIONS should not be retried when catch_partitions_limit=True."""
        cursor = self._make_cursor(num_retries=3)
        cursor._execute = MagicMock(return_value="fake-query-id")
        cursor._collect_result_set = MagicMock(
            return_value=self._make_failed_result("TOO_MANY_OPEN_PARTITIONS")
        )

        with pytest.raises(OperationalError, match="TOO_MANY_OPEN_PARTITIONS"):
            cursor.execute("SELECT 1", catch_partitions_limit=True)

        assert cursor._execute.call_count == 1

    def test_too_many_open_partitions_is_retried_when_catch_disabled(self):
        """TOO_MANY_OPEN_PARTITIONS should be retried when catch_partitions_limit=False."""
        num_retries = 2
        cursor = self._make_cursor(num_retries=num_retries)
        cursor._execute = MagicMock(return_value="fake-query-id")
        cursor._collect_result_set = MagicMock(
            return_value=self._make_failed_result("TOO_MANY_OPEN_PARTITIONS")
        )

        with pytest.raises(OperationalError, match="TOO_MANY_OPEN_PARTITIONS"):
            cursor.execute("SELECT 1", catch_partitions_limit=False)

        assert cursor._execute.call_count == num_retries + 1

    def test_generic_error_is_retried_by_outer_retry(self):
        """Generic errors should be retried by the outer retry."""
        num_retries = 2
        cursor = self._make_cursor(num_retries=num_retries)
        cursor._execute = MagicMock(return_value="fake-query-id")
        cursor._collect_result_set = MagicMock(
            return_value=self._make_failed_result("GENERIC_ERROR: something went wrong")
        )

        with pytest.raises(OperationalError, match="GENERIC_ERROR"):
            cursor.execute("SELECT 1")

        assert cursor._execute.call_count == num_retries + 1
