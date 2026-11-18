from multiprocessing import get_context
from unittest.mock import Mock, patch

import google.cloud.bigquery
import pytest

from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.contracts.connection import ConnectionState


REUSE_MESSAGE = "BigQuery client reuse is enabled"


def _credentials(**kwargs) -> BigQueryCredentials:
    return BigQueryCredentials(method="oauth", database="project", schema="dataset", **kwargs)


def _manager(reuse_connections) -> BigQueryConnectionManager:
    credentials = (
        _credentials()
        if reuse_connections is None
        else _credentials(reuse_connections=reuse_connections)
    )
    return BigQueryConnectionManager(
        profile=Mock(credentials=credentials, query_comment=None),
        mp_context=get_context("spawn"),
    )


def _run_node(manager: BigQueryConnectionManager, name: str):
    """Acquire this thread's connection for a node, use its client, then release it."""
    connection = manager.set_connection_name(name)
    client = connection.handle
    manager.release()
    return connection, client


@pytest.fixture
def client_factory():
    with patch(
        "dbt.adapters.bigquery.connections.create_bigquery_client",
        side_effect=lambda credentials: Mock(google.cloud.bigquery.Client),
    ) as factory:
        yield factory


class TestReuseConnectionsCredentials:
    def test_defaults_to_unset(self):
        assert _credentials().reuse_connections is None

    def test_is_reported_as_a_connection_key(self):
        assert "reuse_connections" in _credentials()._connection_keys()


class TestReuseConnectionsRelease:
    def test_client_is_kept_open_between_nodes_when_enabled(self, client_factory):
        manager = _manager(reuse_connections=True)

        connection, first_client = _run_node(manager, "model.a")
        assert connection.state == ConnectionState.OPEN
        first_client.close.assert_not_called()

        connection, second_client = _run_node(manager, "model.b")
        assert second_client is first_client
        assert connection.name == "model.b"
        assert client_factory.call_count == 1

    @pytest.mark.parametrize("reuse_connections", [False, None])
    def test_client_is_closed_after_each_node_when_not_enabled(
        self, client_factory, reuse_connections
    ):
        manager = _manager(reuse_connections=reuse_connections)

        connection, first_client = _run_node(manager, "model.a")
        assert connection.state == ConnectionState.CLOSED
        first_client.close.assert_called_once()

        _, second_client = _run_node(manager, "model.b")
        assert second_client is not first_client
        assert client_factory.call_count == 2

    def test_cleanup_all_closes_a_reused_client(self, client_factory):
        manager = _manager(reuse_connections=True)
        _, client = _run_node(manager, "model.a")

        manager.cleanup_all()

        client.close.assert_called_once()
        assert manager.thread_connections == {}


class TestReuseConnectionsFailedOpen:
    def test_failed_connection_is_discarded_on_release(self, client_factory):
        manager = _manager(reuse_connections=True)
        client_factory.side_effect = Exception("cannot create client")

        connection = manager.set_connection_name("model.a")
        with pytest.raises(FailedToConnectError):
            connection.handle
        assert connection.state == ConnectionState.FAIL

        manager.release()

        assert manager.get_if_exists() is None
        manager.cleanup_all()


class TestReuseConnectionsLogging:
    def test_logs_once_when_enabled(self, client_factory):
        with patch("dbt.adapters.bigquery.connections.logger") as logger:
            manager = _manager(reuse_connections=True)
            _run_node(manager, "model.a")
            _run_node(manager, "model.b")

        logger.info.assert_called_once_with(REUSE_MESSAGE)

    @pytest.mark.parametrize("reuse_connections", [False, None])
    def test_does_not_log_when_not_enabled(self, client_factory, reuse_connections):
        with patch("dbt.adapters.bigquery.connections.logger") as logger:
            manager = _manager(reuse_connections=reuse_connections)
            _run_node(manager, "model.a")

        assert REUSE_MESSAGE not in [call.args[0] for call in logger.info.call_args_list]
