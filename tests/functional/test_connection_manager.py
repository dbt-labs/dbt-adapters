from unittest import TestCase, mock

from dbt.adapters.contracts.connection import Connection, Identifier
from dbt_common.helper_types import Port
import psycopg2

from dbt.adapters.postgres import PostgresCredentials, PostgresConnectionManager


class TestConnectionManagerOpen(TestCase):
    connection = None

    # Postgres-specific
    def setUp(self):
        self.connection = self.get_connection()

    def get_connection(self) -> Connection:
        if connection := self.connection:
            pass
        else:
            credentials = PostgresCredentials(
                host="localhost",
                user="test-user",
                port=Port(1111),
                password="test-password",
                database="test-db",
                schema="test-schema",
                retries=2,
            )
            connection = Connection(Identifier("postgres"), None, credentials)
        return connection

    def test_open(self):
        """Test opening a Postgres Connection with failures in the first 3 attempts.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a mock connect to raise a psycopg2.errors.ConnectionFailuer
        in the first 3 invocations, after which the mock should return True. As a result:
        * The Connection state should be "open" and the handle True, as connect
          returns in the 4th attempt.
        * The resulting attempt count should be 4.
        """
        conn = self.connection
        attempt = 0

        def connect(*args, **kwargs):
            nonlocal attempt
            attempt += 1

            if attempt <= 2:
                raise psycopg2.errors.ConnectionFailure("Connection has failed")

            return True

        with mock.patch("psycopg2.connect", wraps=connect) as mock_connect:
            PostgresConnectionManager.open(conn)

            assert mock_connect.call_count == 3

        assert attempt == 3
        assert conn.state == "open"
        assert conn.handle is True
