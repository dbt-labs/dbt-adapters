from unittest.mock import Mock, call, patch

import pytest

from dbt.adapters.base.connections import BaseConnectionManager
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.exceptions import FailedToConnectError


def make_connection():
    connection = Mock()
    connection.handle = None
    connection.state = ConnectionState.INIT
    return connection


def test_retry_connection_retries_until_success():
    connection = make_connection()
    logger = Mock()
    connect = Mock(side_effect=[ValueError("transient"), ValueError("transient"), object()])
    timeout = Mock(side_effect=[0, 1, 2])

    with patch("dbt.adapters.base.connections.sleep") as sleep:
        result = BaseConnectionManager.retry_connection(
            connection=connection,
            connect=connect,
            logger=logger,
            retryable_exceptions=(ValueError,),
            retry_limit=2,
            retry_timeout=timeout,
        )

    assert result is connection
    assert connection.state == ConnectionState.OPEN
    assert connect.call_count == 3
    assert timeout.call_args_list == [call(0), call(1), call(2)]
    assert sleep.call_args_list == [call(0), call(1)]


def test_retry_connection_handles_retry_limits_above_recursion_limit():
    connection = make_connection()
    logger = Mock()
    retry_limit = 1100

    with patch("dbt.adapters.base.connections.sleep"):
        with pytest.raises(FailedToConnectError, match="transient"):
            BaseConnectionManager.retry_connection(
                connection=connection,
                connect=Mock(side_effect=ValueError("transient")),
                logger=logger,
                retryable_exceptions=(ValueError,),
                retry_limit=retry_limit,
                retry_timeout=0,
            )

    assert connection.state == ConnectionState.FAIL
    assert connection.handle is None
