from multiprocessing import get_context
from unittest import mock

import pytest

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from tests.unit.utils import config_from_parts_or_dicts, inject_adapter


def _adapter(autocommit):
    profile_cfg = {
        "outputs": {
            "test": {
                "type": "redshift",
                "dbname": "redshift",
                "user": "root",
                "host": "thishostshouldnotexist.test.us-east-1",
                "pass": "password",
                "port": 5439,
                "schema": "public",
                "autocommit": autocommit,
            }
        },
        "target": "test",
    }
    project_cfg = {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "config-version": 2,
    }
    config = config_from_parts_or_dicts(project_cfg, profile_cfg)
    adapter = RedshiftAdapter(config, get_context("spawn"))
    inject_adapter(adapter, RedshiftPlugin)
    return adapter, config


@pytest.mark.parametrize(
    "flag_enabled, autocommit, expected",
    [
        (True, True, True),
        # The behavior flag has no effect when autocommit is off: dbt still sends
        # BEGIN/COMMIT, so statements run inside a transaction block.
        (True, False, False),
        (False, True, False),
        (False, False, False),
    ],
)
def test_skip_transaction_statements(flag_enabled, autocommit, expected, mocker):
    adapter, config = _adapter(autocommit)
    adapter.behavior.redshift_skip_autocommit_transaction_statements = mocker.MagicMock(
        no_warn=flag_enabled
    )
    connection = mock.MagicMock()
    connection.credentials = config.credentials

    with mock.patch.object(adapter.connections, "get_thread_connection", return_value=connection):
        assert adapter.skip_transaction_statements() is expected


@pytest.mark.parametrize("flag_enabled", [True, False])
def test_skip_transaction_statements_matches_connection_manager(flag_enabled, mocker):
    # The macro-facing method and the BEGIN/COMMIT/ROLLBACK skipping must never disagree.
    adapter, config = _adapter(autocommit=False)
    adapter.behavior.redshift_skip_autocommit_transaction_statements = mocker.MagicMock(
        no_warn=flag_enabled
    )
    connection = mock.MagicMock()
    connection.credentials = config.credentials
    connection.transaction_open = False

    with mock.patch.object(adapter.connections, "get_thread_connection", return_value=connection):
        with mock.patch.object(adapter.connections, "add_begin_query") as add_begin:
            adapter.connections.begin()

    # autocommit is off, so BEGIN is sent whatever the flag says...
    add_begin.assert_called_once()
    # ...and the macro-facing method reports that statements are NOT outside a transaction.
    with mock.patch.object(adapter.connections, "get_thread_connection", return_value=connection):
        assert adapter.skip_transaction_statements() is False


def test_skip_transaction_statements_is_available_to_jinja():
    assert "skip_transaction_statements" in RedshiftAdapter._available_
