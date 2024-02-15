import dataclasses
from multiprocessing import get_context
from unittest import TestCase, mock

import agate
from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.relation import Path
from dbt_common.context import set_invocation_context
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.postgres import Plugin as PostgresPlugin, PostgresAdapter
from tests.unit.utils import (
    config_from_parts_or_dicts,
    inject_adapter,
    mock_connection,
)


class TestPostgresAdapter(TestCase):
    def setUp(self):
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "postgres",
                    "dbname": "postgres",
                    "user": "root",
                    "host": "thishostshouldnotexist",
                    "pass": "password",
                    "port": 5432,
                    "schema": "public",
                }
            },
            "target": "test",
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.mp_context = get_context("spawn")
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = PostgresAdapter(self.config, self.mp_context)
            inject_adapter(self._adapter, PostgresPlugin)
        return self._adapter

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_acquire_connection_validations(self, psycopg2):
        try:
            connection = self.adapter.acquire_connection("dummy")
        except DbtValidationError as e:
            self.fail("got DbtValidationError: {}".format(str(e)))
        except BaseException as e:
            self.fail("acquiring connection failed with unknown exception: {}".format(str(e)))
        self.assertEqual(connection.type, "postgres")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once()

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_acquire_connection(self, psycopg2):
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        self.assertEqual(connection.state, "open")
        self.assertNotEqual(connection.handle, None)
        psycopg2.connect.assert_called_once()

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection("master")
        model = mock_connection("model")
        key = self.adapter.connections.get_thread_identifier()
        model.handle.get_backend_pid.return_value = 42
        self.adapter.connections.thread_connections.update(
            {
                key: master,
                1: model,
            }
        )
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with("select pg_terminate_backend(42)")

        master.handle.get_backend_pid.assert_not_called()

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_default_connect_timeout(self, psycopg2):
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_changed_connect_timeout(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(connect_timeout=30)
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=30,
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_default_keepalive(self, psycopg2):
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_changed_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(keepalives_idle=256)
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            keepalives_idle=256,
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_default_application_name(self, psycopg2):
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_changed_application_name(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(application_name="myapp")
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="myapp",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_role(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(role="somerole")
        connection = self.adapter.acquire_connection("dummy")

        cursor = connection.handle.cursor()

        cursor.execute.assert_called_once_with("set role somerole")

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_search_path(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(search_path="test")
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="dbt",
            options="-c search_path=test",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_sslmode(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(sslmode="require")
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            sslmode="require",
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_ssl_parameters(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(sslmode="verify-ca")
        self.config.credentials = self.config.credentials.replace(sslcert="service.crt")
        self.config.credentials = self.config.credentials.replace(sslkey="service.key")
        self.config.credentials = self.config.credentials.replace(sslrootcert="ca.crt")
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            sslmode="verify-ca",
            sslcert="service.crt",
            sslkey="service.key",
            sslrootcert="ca.crt",
            application_name="dbt",
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_schema_with_space(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(search_path="test test")
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="dbt",
            options="-c search_path=test\ test",  # noqa: [W605]
        )

    @mock.patch("dbt.adapters.postgres.connections.psycopg2")
    def test_set_zero_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(keepalives_idle=0)
        connection = self.adapter.acquire_connection("dummy")

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname="postgres",
            user="root",
            host="thishostshouldnotexist",
            password="password",
            port=5432,
            connect_timeout=10,
            application_name="dbt",
        )

    @mock.patch.object(PostgresAdapter, "execute_macro")
    @mock.patch.object(PostgresAdapter, "_get_catalog_relations")
    def test_get_catalog_various_schemas(self, mock_get_relations, mock_execute):
        self.catalog_test(mock_get_relations, mock_execute, False)

    @mock.patch.object(PostgresAdapter, "execute_macro")
    @mock.patch.object(PostgresAdapter, "_get_catalog_relations")
    def test_get_filtered_catalog(self, mock_get_relations, mock_execute):
        self.catalog_test(mock_get_relations, mock_execute, True)

    def catalog_test(self, mock_get_relations, mock_execute, filtered=False):
        column_names = ["table_database", "table_schema", "table_name"]
        relations = [
            BaseRelation(path=Path(database="dbt", schema="foo", identifier="bar")),
            BaseRelation(path=Path(database="dbt", schema="FOO", identifier="baz")),
            BaseRelation(path=Path(database="dbt", schema=None, identifier="bar")),
            BaseRelation(path=Path(database="dbt", schema="quux", identifier="bar")),
            BaseRelation(path=Path(database="dbt", schema="skip", identifier="bar")),
        ]
        rows = list(map(lambda x: dataclasses.astuple(x.path), relations))
        mock_execute.return_value = agate.Table(rows=rows, column_names=column_names)

        mock_get_relations.return_value = relations

        relation_configs = []
        used_schemas = {("dbt", "foo"), ("dbt", "quux")}

        set_invocation_context({})
        if filtered:
            catalog, exceptions = self.adapter.get_filtered_catalog(
                relation_configs, used_schemas, set([relations[0], relations[3]])
            )
        else:
            catalog, exceptions = self.adapter.get_catalog(relation_configs, used_schemas)

        tupled_catalog = set(map(tuple, catalog))
        if filtered:
            self.assertEqual(tupled_catalog, {rows[0], rows[3]})
        else:
            self.assertEqual(tupled_catalog, {rows[0], rows[1], rows[3]})

        self.assertEqual(exceptions, [])
