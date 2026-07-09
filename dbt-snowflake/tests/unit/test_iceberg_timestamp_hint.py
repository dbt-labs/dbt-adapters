import multiprocessing
from unittest.mock import Mock

import pytest
import snowflake.connector.errors
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.snowflake.connections import SnowflakeConnectionManager


def _connection_manager():
    # exception_handler does not touch the profile, so a Mock profile is sufficient.
    return SnowflakeConnectionManager(Mock(), multiprocessing.get_context("spawn"))


def test_exception_handler_appends_iceberg_hint_for_091385():
    mgr = _connection_manager()
    err = snowflake.connector.errors.ProgrammingError(
        msg=(
            "091385 (42601): Invalid time type scale specified for column 'TS' "
            "with data type 'TIMESTAMP_NTZ(9)'."
        )
    )
    with pytest.raises(DbtDatabaseError) as excinfo:
        with mgr.exception_handler("create iceberg table t as (select ...)"):
            raise err

    message = str(excinfo.value)
    # the original Snowflake error is preserved
    assert "091385" in message
    # and an actionable hint is appended
    assert "TIMESTAMP_NTZ(6)" in message
    assert "nanosecond timestamp support enabled" in message


def test_exception_handler_does_not_append_hint_for_other_errors():
    mgr = _connection_manager()
    err = snowflake.connector.errors.ProgrammingError(
        msg="002003 (42S02): Object 'X' does not exist or not authorized."
    )
    with pytest.raises(DbtDatabaseError) as excinfo:
        with mgr.exception_handler("select * from x"):
            raise err

    message = str(excinfo.value)
    assert "002003" in message
    assert "TIMESTAMP_NTZ(6)" not in message
