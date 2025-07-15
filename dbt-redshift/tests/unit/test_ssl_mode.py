from multiprocessing import get_context
from unittest import TestCase, mock
from unittest.mock import MagicMock

import redshift_connector

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from dbt.adapters.redshift.connections import RedshiftSSLConfig
from tests.unit.utils import config_from_parts_or_dicts, inject_adapter


DEFAULT_SSL_CONFIG = RedshiftSSLConfig().to_dict()
DEFAULT_TCP_KEEPALIVE_CONFIG = {"tcp_keepalive": True}


class TestSSLMode(TestCase):
    def setUp(self):
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
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = RedshiftAdapter(self.config, get_context("spawn"))
            inject_adapter(self._adapter, RedshiftPlugin)
        return self._adapter

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_disable(self):
        self.config.credentials.sslmode = "disable"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region=None,
            timeout=None,
            ssl=False,
            sslmode=None,
            is_serverless=False,
            tcp_keepalive=True,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_allow(self):
        self.config.credentials.sslmode = "allow"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region=None,
            timeout=None,
            ssl=True,
            sslmode="verify-ca",
            is_serverless=False,
            tcp_keepalive=True,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_verify_full(self):
        self.config.credentials.sslmode = "verify-full"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region=None,
            timeout=None,
            ssl=True,
            sslmode="verify-full",
            is_serverless=False,
            tcp_keepalive=True,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_verify_ca(self):
        self.config.credentials.sslmode = "verify-ca"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region=None,
            timeout=None,
            ssl=True,
            sslmode="verify-ca",
            is_serverless=False,
            tcp_keepalive=True,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_prefer(self):
        self.config.credentials.sslmode = "prefer"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region=None,
            timeout=None,
            ssl=True,
            sslmode="verify-ca",
            is_serverless=False,
            tcp_keepalive=True,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_connection_timeout(self):
        self.config.credentials = self.config.credentials.replace(connect_timeout=30)
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region=None,
            timeout=30,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
            is_serverless=False,
        )
