from unittest.mock import MagicMock, patch

import pytest
from pyathena.error import OperationalError
from pyathena.util import RetryConfig

from dbt.adapters.athena.connections import AthenaCursor


@pytest.fixture()
def athena_cursor():
    connection = MagicMock()
    connection.cursor_kwargs = {"num_iceberg_retries": 0}
    retry_config = RetryConfig(attempt=3, max_delay=0, exponential_base=1)
    cursor = AthenaCursor(
        connection=connection,
        converter=MagicMock(),
        formatter=MagicMock(),
        retry_config=retry_config,
        s3_staging_dir="s3://test/",
        schema_name="test_schema",
        catalog_name="test_catalog",
        work_group="test_wg",
        poll_interval=0,
        encryption_option=None,
        kms_key=None,
        kill_on_interrupt=False,
        result_reuse_enable=False,
        result_reuse_minutes=0,
    )
    return cursor


class TestAthenaCursorRetry:
    def test_retry_on_query_timeout_by_default(self, athena_cursor):
        with patch.object(
            athena_cursor, "_execute", side_effect=OperationalError("Query timeout")
        ):
            with pytest.raises(OperationalError, match="Query timeout"):
                athena_cursor.execute("SELECT 1")
            assert athena_cursor._execute.call_count == 3

    def test_no_retry_on_query_timeout_when_skip_enabled(self, athena_cursor):
        athena_cursor.connection.cursor_kwargs["skip_retry_on_query_timeout"] = True
        with patch.object(
            athena_cursor, "_execute", side_effect=OperationalError("Query timeout")
        ):
            with pytest.raises(OperationalError, match="Query timeout"):
                athena_cursor.execute("SELECT 1")
            assert athena_cursor._execute.call_count == 1

    def test_no_retry_on_query_exhausted_resources(self, athena_cursor):
        with patch.object(
            athena_cursor, "_execute", side_effect=OperationalError("Query exhausted resources")
        ):
            with pytest.raises(OperationalError, match="Query exhausted resources"):
                athena_cursor.execute("SELECT 1")
            assert athena_cursor._execute.call_count == 1

    def test_no_retry_on_too_many_open_partitions_when_catch_enabled(self, athena_cursor):
        with patch.object(
            athena_cursor, "_execute", side_effect=OperationalError("TOO_MANY_OPEN_PARTITIONS")
        ):
            with pytest.raises(OperationalError, match="TOO_MANY_OPEN_PARTITIONS"):
                athena_cursor.execute("SELECT 1", catch_partitions_limit=True)
            assert athena_cursor._execute.call_count == 1

    def test_retry_on_too_many_open_partitions_when_catch_disabled(self, athena_cursor):
        with patch.object(
            athena_cursor, "_execute", side_effect=OperationalError("TOO_MANY_OPEN_PARTITIONS")
        ):
            with pytest.raises(OperationalError, match="TOO_MANY_OPEN_PARTITIONS"):
                athena_cursor.execute("SELECT 1", catch_partitions_limit=False)
            assert athena_cursor._execute.call_count == 3

    def test_retry_on_transient_error(self, athena_cursor):
        with patch.object(
            athena_cursor, "_execute", side_effect=OperationalError("Some transient error")
        ):
            with pytest.raises(OperationalError, match="Some transient error"):
                athena_cursor.execute("SELECT 1")
            assert athena_cursor._execute.call_count == 3
