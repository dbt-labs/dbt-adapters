from unittest import mock

import pytest

from dbt.adapters.athena.connections import AthenaConnection, AthenaCredentials

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
