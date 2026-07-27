import requests

from multiprocessing import get_context
from unittest import TestCase, mock
from unittest.mock import MagicMock

from dbt.adapters.exceptions import FailedToConnectError
import redshift_connector

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from dbt.adapters.redshift.connections import get_connection_method, RedshiftSSLConfig
from tests.unit.utils import config_from_parts_or_dicts, inject_adapter


DEFAULT_SSL_CONFIG = RedshiftSSLConfig().to_dict()
DEFAULT_TCP_KEEPALIVE_CONFIG = {"tcp_keepalive": True}


class AuthMethod(TestCase):
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


class TestInvalidMethod(AuthMethod):
    def test_invalid_auth_method(self):
        # we have to set method this way, otherwise it won't validate
        self.config.credentials.method = "badmethod"  # type: ignore
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = get_connection_method(self.config.credentials)  # type: ignore
            connect_method_factory.get_connect_method()
        self.assertTrue("badmethod" in context.exception.msg)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_missing_region_failure(self):
        # Failure test with no region
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233_no_region",
            region=None,
        )

        with self.assertRaises(FailedToConnectError):
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233_no_region",
                database="redshift",
                cluster_identifier=None,
                auto_create=False,
                db_groups=[],
                db_user="root",
                password="",
                user="",
                profile="test",
                timeout=None,
                port=5439,
                is_serverless=False,
                **DEFAULT_SSL_CONFIG,
            )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_invalid_region_failure(self):
        # Invalid region test
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233_no_region.us-not-a-region-1",
            region=None,
        )

        with self.assertRaises(FailedToConnectError):
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233_no_region",
                database="redshift",
                cluster_identifier=None,
                auto_create=False,
                db_groups=[],
                db_user="root",
                password="",
                user="",
                profile="test",
                timeout=None,
                port=5439,
                **DEFAULT_SSL_CONFIG,
            )


class TestDatabaseMethod(AuthMethod):
    @mock.patch("redshift_connector.connect", MagicMock())
    def test_default(self):
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
            timeout=None,
            region=None,
            is_serverless=False,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_explicit_auth_method(self):
        self.config.method = "database"

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
            is_serverless=False,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    def test_database_verification_is_case_insensitive(self):
        # Override adapter settings from setUp()
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "redshift",
                    "dbname": "Redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist",
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
        self.adapter.cleanup_connections()
        self._adapter = RedshiftAdapter(self.config, get_context("spawn"))
        self.adapter.verify_database("redshift")


class TestIAMUserMethod(AuthMethod):

    def test_iam_optionals(self):
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "redshift",
                    "dbname": "redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist",
                    "port": 5439,
                    "schema": "public",
                    "method": "iam",
                    "cluster_id": "my_redshift",
                    "db_groups": ["my_dbgroup"],
                    "autocreate": True,
                }
            },
            "target": "test",
        }

        config_from_parts_or_dicts(self.config, profile_cfg)

    def test_no_cluster_id(self):
        self.config.credentials = self.config.credentials.replace(method="iam")
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = get_connection_method(self.config.credentials)
            connect_method_factory.get_connect_method()

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_default(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            cluster_id="my_redshift",
            host="thishostshouldnotexist.test.us-east-1",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            db_user="root",
            password="",
            user="",
            cluster_identifier="my_redshift",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            profile=None,
            port=5439,
            is_serverless=False,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            cluster_id="my_redshift",
            iam_profile="test",
            host="thishostshouldnotexist.test.us-east-1",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle

        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            cluster_identifier="my_redshift",
            region=None,
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            is_serverless=False,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_explicit(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            cluster_id="my_redshift",
            host="thishostshouldnotexist.test.us-east-1",
            access_key_id="my_access_key_id",
            secret_access_key="my_secret_access_key",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            access_key_id="my_access_key_id",
            secret_access_key="my_secret_access_key",
            database="redshift",
            db_user="root",
            password="",
            user="",
            cluster_identifier="my_redshift",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            port=5439,
            is_serverless=False,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_explicit_workgroup_name(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            host="thishostshouldnotexist.test.amazonaws.com",
            access_key_id="my_access_key_id",
            secret_access_key="my_secret_access_key",
            is_serverless=True,
            region="us-east-1",
            user="test_user",
            serverless_work_group="my_workgroup",
            serverless_acct_id="0123456789",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.amazonaws.com",
            access_key_id="my_access_key_id",
            secret_access_key="my_secret_access_key",
            database="redshift",
            db_user="test_user",
            user="",
            password="",
            cluster_identifier=None,
            region="us-east-1",
            timeout=None,
            auto_create=False,
            db_groups=[],
            port=5439,
            is_serverless=True,
            serverless_work_group="my_workgroup",
            serverless_acct_id="0123456789",
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )


class TestIAMUserMethodServerless(AuthMethod):

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_default_region(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_explicit_serverless(self):
        host = "doesnotexist.custom-domain.com"
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host=host,
            is_serverless=True,
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host=host,
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_explicit_region(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.redshift-serverless.amazonaws.com",
            region="us-east-2",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region="us-east-2",
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_invalid_serverless(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
                database="redshift",
                cluster_identifier=None,
                region=None,
                auto_create=False,
                db_groups=[],
                db_user="root",
                password="",
                user="",
                profile="test",
                port=5439,
                timeout=None,
                **DEFAULT_SSL_CONFIG,
                **DEFAULT_TCP_KEEPALIVE_CONFIG,
            )
        self.assertTrue("'host' must be provided" in context.exception.msg)


class TestIAMRoleMethod(AuthMethod):

    def test_no_cluster_id(self):
        self.config.credentials = self.config.credentials.replace(method="iam_role")
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = get_connection_method(self.config.credentials)
            connect_method_factory.get_connect_method()

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_default(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            cluster_id="my_redshift",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            cluster_identifier="my_redshift",
            db_user=None,
            password="",
            user="",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            port=5439,
            group_federation=True,
            is_serverless=False,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            cluster_id="my_redshift",
            iam_profile="test",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            cluster_identifier="my_redshift",
            db_user=None,
            password="",
            user="",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            profile="test",
            port=5439,
            group_federation=True,
            is_serverless=False,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )


class TestIAMRoleMethodServerless(AuthMethod):
    # Should behave like IAM Role provisioned, with the exception of not having group_federation set

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_default_region(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            iam_profile="iam_profile_test",
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            db_user=None,
            password="",
            user="",
            profile="iam_profile_test",
            timeout=None,
            port=5439,
            group_federation=False,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_ignore_cluster(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            iam_profile="iam_profile_test",
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            cluster_id="my_redshift",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            db_user=None,
            password="",
            user="",
            profile="iam_profile_test",
            timeout=None,
            port=5439,
            group_federation=False,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_explicit_region(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            iam_profile="iam_profile_test",
            host="doesnotexist.1233.redshift-serverless.amazonaws.com",
            region="us-east-2",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region="us-east-2",
            auto_create=False,
            db_groups=[],
            db_user=None,
            password="",
            user="",
            profile="iam_profile_test",
            timeout=None,
            port=5439,
            group_federation=False,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_invalid_serverless(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            iam_profile="iam_profile_test",
            host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
                database="redshift",
                cluster_identifier=None,
                region=None,
                auto_create=False,
                db_groups=[],
                db_user=None,
                password="",
                user="",
                profile="iam_profile_test",
                port=5439,
                timeout=None,
                group_federation=False,
                is_serverless=True,
                serverless_work_group=None,
                serverless_acct_id=None,
                **DEFAULT_SSL_CONFIG,
                **DEFAULT_TCP_KEEPALIVE_CONFIG,
            )
        self.assertTrue("'host' must be provided" in context.exception.msg)


class TestIAMIdcBrowser(AuthMethod):
    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_idc_browser_all_fields(self):
        self.config.credentials = self.config.credentials.replace(
            method="browser_identity_center",
            idc_region="us-east-1",
            issuer_url="https://identitycenter.amazonaws.com/ssoins-randomchars",
            idc_client_display_name="display name",
            idp_response_timeout=0,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            idp_listen_port=1111,
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=False,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            password="",
            user="",
            timeout=None,
            port=5439,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            idp_response_timeout=0,
            idc_client_display_name="display name",
            credentials_provider="BrowserIdcAuthPlugin",
            idc_region="us-east-1",
            issuer_url="https://identitycenter.amazonaws.com/ssoins-randomchars",
            listen_port=1111,
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_idc_browser_required_fields_only(self):
        self.config.credentials = self.config.credentials.replace(
            method="browser_identity_center",
            idc_region="us-east-1",
            issuer_url="https://identitycenter.amazonaws.com/ssoins-randomchars",
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=False,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            password="",
            user="",
            timeout=None,
            port=5439,
            is_serverless=True,
            serverless_work_group=None,
            serverless_acct_id=None,
            **DEFAULT_SSL_CONFIG,
            credentials_provider="BrowserIdcAuthPlugin",
            listen_port=7890,
            idp_response_timeout=60,
            idc_client_display_name="Amazon Redshift driver",
            idc_region="us-east-1",
            issuer_url="https://identitycenter.amazonaws.com/ssoins-randomchars",
            **DEFAULT_TCP_KEEPALIVE_CONFIG,
        )

    def test_invalid_adapter_missing_fields(self):
        self.config.credentials = self.config.credentials.replace(
            method="browser_identity_center",
            idp_listen_port=1111,
            idc_client_display_name="my display",
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=False,
                host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
                database="redshift",
                cluster_identifier=None,
                region=None,
                auto_create=False,
                db_groups=[],
                password="",
                user="",
                timeout=None,
                port=5439,
                is_serverless=True,
                **DEFAULT_SSL_CONFIG,
                credentials_provider="BrowserIdcAuthPlugin",
                listen_port=1111,
                idp_response_timeout=60,
                idc_client_display_name="my display",
                **DEFAULT_TCP_KEEPALIVE_CONFIG,
            )

        assert (
            "'idc_region', 'issuer_url' field(s) are required for 'browser_identity_center' credentials method"
            in context.exception.msg
        )


class TestIAMIdcToken(AuthMethod):
    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_idc_token_all_required_fields_okta(self):
        """This test doesn't follow the idiom elsewhere in this file because we
        a real test would need a valid refresh token which would require a valid
        authorization request, neither of which are possible in automated testing at
        merge. This is a surrogate test.
        """
        self.config.credentials = self.config.credentials.replace(
            method="oauth_token_identity_center",
            token_endpoint={
                "type": "okta",
                "request_url": "https://dbtcs.oktapreview.com/oauth2/default/v1/token",
                "idp_auth_credentials": "my_auth_creds",
                "request_data": "grant_type=refresh_token&redirect_uri=http%3A%2F%2Flocalhost%3A8080%2Flogin%2Foauth2%2Fcode%2Fokta&refresh_token=my_token",
            },
        )
        with self.assertRaises(requests.exceptions.HTTPError) as context:
            """
            An http says we've made it in operation to call the token request which fails
            due to invalid refresh token and auth creds
            """
            connection = self.adapter.acquire_connection("dummy")
            connection.handle

        assert "401 Client Error: Unauthorized for url" in str(context.exception)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_idc_token_all_required_fields_entra(self):
        """This test doesn't follow the idiom elsewhere in this file because we
        a real test would need a valid refresh token which would require a valid
        authorization request, neither of which are possible in automated testing at
        merge. This is a surrogate test.
        """
        self.config.credentials = self.config.credentials.replace(
            method="oauth_token_identity_center",
            token_endpoint={
                "type": "entra",
                "request_url": "https://login.microsoftonline.com/my_tenant/oauth2/v2.0/token",
                "request_data": "my_data",
            },
        )
        with self.assertRaises(requests.exceptions.HTTPError) as context:
            """
            An http says we've made it in operation to call the token request which fails
            due to invalid refresh token and auth creds
            """
            connection = self.adapter.acquire_connection("dummy")
            connection.handle

        assert "400 Client Error: Bad Request for url" in str(context.exception)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_invalid_idc_token_missing_field(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="oauth_token_identity_center",
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
        assert (
            "'token_endpoint' field(s) are required for 'oauth_token_identity_center' credentials method"
            in context.exception.msg
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_invalid_idc_token_missing_token_endpoint_subfield_okta(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="oauth_token_identity_center",
            token_endpoint={
                "type": "okta",
                "request_data": "my_data",
                "idp_auth_credentials": "my_auth_creds",
            },
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
        assert "Missing required key in token_endpoint: 'request_url'" in context.exception.msg

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_invalid_idc_token_missing_token_endpoint_subfield_entra(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="oauth_token_identity_center",
            token_endpoint={
                "type": "entra",
                "request_url": "https://dbtcs.oktapreview.com/oauth2/default/v1/token",
            },
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
        assert "Missing required key in token_endpoint: 'request_data'" in context.exception.msg

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_invalid_idc_token_missing_token_endpoint_type(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="oauth_token_identity_center",
            token_endpoint={},
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
        assert "Missing required key in token_endpoint: 'type'" in context.exception.msg
