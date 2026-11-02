from multiprocessing import get_context
from unittest import mock

import pytest
from snowflake import connector as snowflake_connector

from dbt.adapters.snowflake.connections import (
    LARGE_STATEMENT_RENEWAL_CHARS,
    SnowflakeConnectionManager,
)

from .utils import config_from_parts_or_dicts

LARGE_SQL = "insert into t values ('" + "x" * LARGE_STATEMENT_RENEWAL_CHARS + "')"


@pytest.fixture
def handle():
    handle = mock.MagicMock(spec=snowflake_connector.SnowflakeConnection)
    cursor = handle.cursor.return_value
    cursor.execute.return_value = mock.MagicMock(sfqid="42")
    cursor.stats = None
    cursor.rowcount = 1
    handle.is_valid.return_value = True
    with mock.patch(
        "dbt.adapters.snowflake.connections.snowflake.connector.connect", return_value=handle
    ):
        yield handle


@pytest.fixture
def manager(handle):
    config = config_from_parts_or_dicts(
        {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        },
        {
            "outputs": {
                "test": {
                    "type": "snowflake",
                    "account": "test_account",
                    "user": "test_user",
                    "database": "test_database",
                    "warehouse": "test_warehouse",
                    "schema": "public",
                },
            },
            "target": "test",
        },
    )
    manager = SnowflakeConnectionManager(config, get_context("spawn"))
    manager.set_connection_name("master")
    return manager


def test_large_statement_is_preceded_by_a_heartbeat(manager, handle):
    manager.add_query("select 1")
    handle.reset_mock()

    manager.add_query(LARGE_SQL)

    handle.is_valid.assert_called_once_with()
    calls = [c[0] for c in handle.mock_calls]
    assert calls.index("is_valid") < calls.index("cursor().execute")


def test_small_statement_is_not_preceded_by_a_heartbeat(manager, handle):
    manager.add_query("select 1")

    manager.add_query("insert into t values (1)")

    handle.is_valid.assert_not_called()


def test_large_first_statement_skips_the_heartbeat(manager, handle):
    # the session is opened for this statement, so its token is brand new
    manager.add_query(LARGE_SQL)

    handle.is_valid.assert_not_called()


def test_failed_heartbeat_still_runs_the_statement(manager, handle):
    manager.add_query("select 1")
    handle.reset_mock(return_value=False)
    handle.is_valid.return_value = False

    manager.add_query(LARGE_SQL)

    handle.is_valid.assert_called_once_with()
    handle.cursor.return_value.execute.assert_called_once()
