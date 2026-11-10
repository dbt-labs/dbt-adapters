import os
import pytest
import logging
from importlib import reload
from unittest.mock import Mock, patch, call
import multiprocessing
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt_common.exceptions import DbtConfigError
import dbt.adapters.snowflake.connections as connections
import dbt.adapters.events.logging


class TestSnowflakeLogging:
    """Test suite for snowflake logging configuration"""

    def test_connections_sets_debug_logs_when_env_var_present(self, monkeypatch):
        """Test that setting DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING enables DEBUG level logging"""
        log_mock = Mock()
        logger_mock = Mock(return_value=log_mock)

        monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
        monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": "true"})

        reload(connections)

        # Verify AdapterLogger was created with correct name
        logger_mock.assert_called_once_with("Snowflake")

        # Verify all three dependency loggers are configured with DEBUG level
        expected_debug_calls = [
            call("Setting snowflake.connector to DEBUG (file logging only)"),
            call("Setting botocore to DEBUG (file logging only)"),
            call("Setting boto3 to DEBUG (file logging only)"),
        ]
        log_mock.debug.assert_has_calls(expected_debug_calls)

        expected_level_calls = [
            call("snowflake.connector", "DEBUG"),
            call("botocore", "DEBUG"),
            call("boto3", "DEBUG"),
        ]
        log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_connections_sets_error_logs_when_env_var_absent(self, monkeypatch):
        """Test that absence of DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING sets ERROR level logging"""
        log_mock = Mock()
        logger_mock = Mock(return_value=log_mock)

        monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
        # Explicitly set environment without the debug flag
        monkeypatch.setattr(os, "environ", {})

        with patch("logging.getLogger") as mock_get_logger:
            reload(connections)

        # Verify AdapterLogger was created
        logger_mock.assert_called_once_with("Snowflake")

        # Verify all three dependency loggers are configured with ERROR level
        expected_debug_calls = [
            call("Setting snowflake.connector to ERROR (file logging only)"),
            call("Setting botocore to ERROR (file logging only)"),
            call("Setting boto3 to ERROR (file logging only)"),
        ]

        log_mock.debug.assert_has_calls(expected_debug_calls)

        expected_level_calls = [
            call("snowflake.connector", "ERROR"),
            call("botocore", "ERROR"),
            call("boto3", "ERROR"),
        ]
        log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_connections_handles_various_env_var_values(self, monkeypatch):
        """Test that any truthy value for the env var enables debug logging"""
        test_values = ["1", "True", "YES", "on", "random"]

        for value in test_values:
            log_mock = Mock()
            logger_mock = Mock(return_value=log_mock)

            monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
            monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": value})

            with patch("logging.getLogger"):
                reload(connections)

            # Should set DEBUG level for all loggers (fallback for non-INFO/DEBUG values)
            expected_level_calls = [
                call("snowflake.connector", "DEBUG"),
                call("botocore", "DEBUG"),
                call("boto3", "DEBUG"),
            ]
            log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_connections_handles_case_insensitive_values(self, monkeypatch):
        """Test that DEBUG and INFO values are case insensitive"""
        test_cases = [
            ("debug", "DEBUG"),
            ("DEBUG", "DEBUG"),
            ("info", "INFO"),
            ("INFO", "INFO"),
            ("Debug", "DEBUG"),
            ("Info", "INFO"),
        ]

        for env_value, expected_level in test_cases:
            log_mock = Mock()
            logger_mock = Mock(return_value=log_mock)

            monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
            monkeypatch.setattr(
                os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": env_value}
            )

            with patch("logging.getLogger"):
                reload(connections)

            # Should set the expected level for all loggers
            expected_level_calls = [
                call("snowflake.connector", expected_level),
                call("botocore", expected_level),
                call("boto3", expected_level),
            ]
            log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_setup_snowflake_logging_with_info_level(self):
        """Test setup_snowflake_logging with INFO level"""
        with patch("dbt.adapters.snowflake.connections.logger") as mock_logger:
            with patch("logging.getLogger") as mock_get_logger:
                mock_package_logger = Mock()
                mock_get_logger.return_value = mock_package_logger

                # Test INFO level
                connections.setup_snowflake_logging("INFO")

                # Verify debug messages (always say "DEBUG (file logging only)")
                expected_debug_calls = [
                    call("Setting snowflake.connector to INFO (file logging only)"),
                    call("Setting botocore to INFO (file logging only)"),
                    call("Setting boto3 to INFO (file logging only)"),
                ]
                mock_logger.debug.assert_has_calls(expected_debug_calls)

                # Verify correct level is set (INFO in this case)
                expected_level_calls = [
                    call("snowflake.connector", "INFO"),
                    call("botocore", "INFO"),
                    call("boto3", "INFO"),
                ]
                mock_logger.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_expected_logger_names_are_configured(self):
        """Test that exactly the expected logger names are configured"""
        expected_loggers = ["snowflake.connector", "botocore", "boto3"]

        with patch("dbt.adapters.snowflake.connections.logger") as mock_logger:
            with patch("logging.getLogger") as mock_get_logger:
                connections.setup_snowflake_logging("DEBUG")

                # Extract the logger names from the calls
                actual_logger_names = [
                    call[0][0]
                    for call in mock_logger.set_adapter_dependency_log_level.call_args_list
                ]

                assert actual_logger_names == expected_loggers
                assert len(actual_logger_names) == 3


# Keep existing tests for backward compatibility
def test_connections_sets_logs_in_response_to_env_var(monkeypatch):
    """Test that setting the DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING environment variable happens on import"""
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": "true"})
    reload(connections)

    assert log_mock.debug.call_count == 3
    assert log_mock.set_adapter_dependency_log_level.call_count == 3


def test_connections_does_not_set_logs_in_response_to_env_var(monkeypatch):
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    reload(connections)

    assert log_mock.debug.call_count == 3  # Debug messages are always logged
    assert (
        log_mock.set_adapter_dependency_log_level.call_count == 3
    )  # But with ERROR level (default)


def test_connnections_credentials_replaces_underscores_with_hyphens():
    credentials = {
        "account": "account_id_with_underscores",
        "user": "user",
        "password": "password",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
    }
    creds = connections.SnowflakeCredentials(**credentials)
    assert creds.account == "account-id-with-underscores"


class TestSnowflakeCredentialsReuseConnections:
    """auth_args() never passes reuse_connections to the connector: it is dbt's own
    pooling switch, read by SnowflakeConnectionManager.release()"""

    @pytest.fixture
    def base_credentials(self):
        return {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
            "warehouse": "test_wh",
            "schema": "test_schema",
        }

    def test_reuse_connections_defaults_to_true(self, base_credentials):
        creds = connections.SnowflakeCredentials(**base_credentials)
        assert creds.reuse_connections is True
        assert "reuse_connections" not in creds.auth_args()

    def test_reuse_connections_unset_with_keep_alive_is_not_passed(self, base_credentials):
        # when client_session_keep_alive is set, reuse_connections stays None;
        # passing None to the connector is a hard error on connector 5.x
        creds = connections.SnowflakeCredentials(
            **base_credentials, client_session_keep_alive=True
        )
        assert creds.reuse_connections is None
        assert "reuse_connections" not in creds.auth_args()

    def test_reuse_connections_explicit_false_is_not_passed(self, base_credentials):
        creds = connections.SnowflakeCredentials(
            **base_credentials, client_session_keep_alive=True, reuse_connections=False
        )
        assert creds.reuse_connections is False
        assert "reuse_connections" not in creds.auth_args()

    def test_reuse_connections_explicit_true_is_not_passed(self, base_credentials):
        creds = connections.SnowflakeCredentials(
            **base_credentials, client_session_keep_alive=True, reuse_connections=True
        )
        assert creds.reuse_connections is True
        assert "reuse_connections" not in creds.auth_args()

    @pytest.mark.parametrize(
        "reuse_connections,released",
        [(True, False), (False, True), (None, True)],
    )
    def test_release_honors_reuse_connections(self, reuse_connections, released):
        manager = connections.SnowflakeConnectionManager.__new__(
            connections.SnowflakeConnectionManager
        )
        manager.profile = Mock(credentials=Mock(reuse_connections=reuse_connections))
        with patch.object(connections.SQLConnectionManager, "release") as parent_release:
            manager.release()
        assert parent_release.called is released


class TestSnowflakeCredentialsCertRevocationCheckMode:
    """`cert_revocation_check_mode` maps to the OCSP options on connector 4.x, and to
    `crl_check_mode` on connector 5.x, which has no OCSP"""

    @pytest.fixture
    def base_credentials(self):
        return {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
            "warehouse": "test_wh",
            "schema": "test_schema",
        }

    @pytest.mark.parametrize(
        "connector_major_version,profile_settings,expected_args",
        [
            (4, {}, {"disable_ocsp_checks": False, "ocsp_fail_open": True}),
            (
                4,
                {"cert_revocation_check_mode": "ENABLED"},
                {"disable_ocsp_checks": False, "ocsp_fail_open": False},
            ),
            (
                4,
                {"cert_revocation_check_mode": "DISABLED"},
                {"disable_ocsp_checks": True, "ocsp_fail_open": True},
            ),
            (4, {"insecure_mode": True}, {"disable_ocsp_checks": True, "ocsp_fail_open": True}),
            # CRL checks are opt-in on 5.x: nothing is passed when the mode is unset
            (5, {}, {}),
            (5, {"cert_revocation_check_mode": "ADVISORY"}, {"crl_check_mode": "ADVISORY"}),
            (5, {"cert_revocation_check_mode": "ENABLED"}, {"crl_check_mode": "ENABLED"}),
            (5, {"cert_revocation_check_mode": "DISABLED"}, {"crl_check_mode": "DISABLED"}),
            (5, {"insecure_mode": True}, {"crl_check_mode": "DISABLED"}),
        ],
        ids=[
            "4_default",
            "4_enabled",
            "4_disabled",
            "4_insecure_mode",
            "5_default",
            "5_advisory",
            "5_enabled",
            "5_disabled",
            "5_insecure_mode",
        ],
    )
    def test_revocation_check_args(
        self, base_credentials, connector_major_version, profile_settings, expected_args
    ):
        if connector_major_version == 4:
            expected_args = {**expected_args, "ocsp_root_certs_dict_lock_timeout": 10}
        creds = connections.SnowflakeCredentials(**base_credentials, **profile_settings)
        with patch.object(
            connections, "_connector_major_version", return_value=connector_major_version
        ):
            assert creds.revocation_check_args() == expected_args

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, None),
            ("advisory", "ADVISORY"),
            ("Enabled", "ENABLED"),
            ("DISABLED", "DISABLED"),
        ],
    )
    def test_mode_is_normalized(self, base_credentials, value, expected):
        creds = connections.SnowflakeCredentials(
            **base_credentials, cert_revocation_check_mode=value
        )
        assert creds.cert_revocation_check_mode == expected

    def test_default_mode_is_unset(self, base_credentials):
        creds = connections.SnowflakeCredentials(**base_credentials)
        assert creds.cert_revocation_check_mode is None

    def test_invalid_mode_raises(self, base_credentials):
        with pytest.raises(DbtConfigError, match="Invalid cert_revocation_check_mode 'strict'"):
            connections.SnowflakeCredentials(
                **base_credentials, cert_revocation_check_mode="strict"
            )

    def test_insecure_mode_with_enabled_raises(self, base_credentials):
        with pytest.raises(DbtConfigError, match="cannot be combined"):
            connections.SnowflakeCredentials(
                **base_credentials, insecure_mode=True, cert_revocation_check_mode="ENABLED"
            )

    def test_replace_keeps_settings(self, base_credentials):
        # `replace()` runs `__post_init__` again on values it already normalized
        creds = connections.SnowflakeCredentials(
            **base_credentials, cert_revocation_check_mode="enabled"
        )
        assert creds.replace(user="other_user").cert_revocation_check_mode == "ENABLED"

        insecure = connections.SnowflakeCredentials(**base_credentials).replace(insecure_mode=True)
        with patch.object(connections, "_connector_major_version", return_value=5):
            assert insecure.revocation_check_args() == {"crl_check_mode": "DISABLED"}

    def test_connector_major_version_reads_installed_connector(self):
        assert connections._connector_major_version() in (4, 5)


class TestSnowflakeCredentialsLogoutArgs:
    """Connector 5.x warns when `enable_server_session_keep_alive_auto_detection` is
    unset, so dbt pins its current default; connector 4.x does not have the option"""

    @pytest.mark.parametrize(
        "connector_major_version,expected_args",
        [
            (4, {}),
            (5, {"enable_server_session_keep_alive_auto_detection": True}),
        ],
    )
    def test_logout_args(self, connector_major_version, expected_args):
        creds = connections.SnowflakeCredentials(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
        )
        with patch.object(
            connections, "_connector_major_version", return_value=connector_major_version
        ):
            assert creds.logout_args() == expected_args


class TestSnowflakeCredentialsPATAuth:
    """Test suite for Programmatic Access Token (PAT) authentication"""

    @pytest.fixture
    def base_credentials(self):
        return {
            "account": "test_account",
            "database": "test_db",
            "schema": "test_schema",
        }

    def test_pat_auth_args_passes_token(self, base_credentials):
        """PAT authenticator should pass token directly to connector"""
        creds = connections.SnowflakeCredentials(
            **base_credentials,
            authenticator="programmatic_access_token",
            token="my-pat-token",
        )
        args = creds.auth_args()
        assert args["token"] == "my-pat-token"
        assert args["authenticator"] == "programmatic_access_token"

    def test_pat_auth_no_spurious_warning(self, base_credentials):
        """PAT authenticator should not trigger the 'token set but authenticator wrong' warning"""
        # Patch warn_or_error so we can assert it is NOT called during __post_init__.
        # If 'programmatic_access_token' is ever accidentally removed from the allowlist,
        # __post_init__ would call warn_or_error and this assertion would fail.
        with patch("dbt.adapters.snowflake.connections.warn_or_error") as mock_warn:
            connections.SnowflakeCredentials(
                **base_credentials,
                authenticator="programmatic_access_token",
                token="my-pat-token",
            )
            mock_warn.assert_not_called()

    def test_pat_auth_without_token(self, base_credentials):
        """PAT authenticator without a token should still produce auth args (connector will error)"""
        creds = connections.SnowflakeCredentials(
            **base_credentials,
            authenticator="programmatic_access_token",
        )
        args = creds.auth_args()
        assert args["authenticator"] == "programmatic_access_token"
        assert args.get("token") is None


def test_snowflake_oauth_expired_token_raises_error():
    credentials = {
        "account": "test_account",
        "user": "test_user",
        "authenticator": "oauth",
        "token": "expired_or_incorrect_token",
        "database": "database",
        "schema": "schema",
    }

    mp_context = multiprocessing.get_context("spawn")
    mock_credentials = connections.SnowflakeCredentials(**credentials)

    with patch.object(
        connections.SnowflakeConnectionManager,
        "open",
        side_effect=FailedToConnectError(
            "This error occurs when authentication has expired. "
            "Please reauth with your auth provider."
        ),
    ):

        adapter = connections.SnowflakeConnectionManager(mock_credentials, mp_context)

        with pytest.raises(FailedToConnectError):
            adapter.open()


def test_connnections_credentials_passes_through_wif_params():
    credentials = {
        "account": "account_id_with_underscores",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
        "authenticator": "workload_identity",
        "workload_identity_provider": "azure",
        "workload_identity_entra_resource": "app://123",
        "token": "test_token",
    }
    auth_args = connections.SnowflakeCredentials(**credentials).auth_args()
    assert auth_args["authenticator"] == "WORKLOAD_IDENTITY"
    assert auth_args["workload_identity_provider"] == "azure"
    assert auth_args["workload_identity_entra_resource"] == "app://123"
    assert auth_args["token"] == "test_token"


def test_connnections_credentials_wif_oidc_token_and_no_user_no_warning():
    """OIDC workload identity uses a token and no user; neither should trigger
    the 'token set but authenticator wrong' warning nor the 'user required' error.

    If 'workload_identity' is ever removed from the allowlist in __post_init__,
    warn_or_error would be called and this assertion would fail.
    """
    credentials = {
        "account": "test_account",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
        "authenticator": "workload_identity",
        "workload_identity_provider": "OIDC",
        "token": "oidc_jwt_token",
        # intentionally no 'user' — not required for workload identity
    }
    with patch("dbt.adapters.snowflake.connections.warn_or_error") as mock_warn:
        connections.SnowflakeCredentials(**credentials)
        mock_warn.assert_not_called()


def test_connnections_credentials_workload_identity_authenticator_is_case_insensitive():
    """workload_identity is matched case-insensitively in both __post_init__ validation
    and auth_args (e.g. 'WORKLOAD_IDENTITY'). Other authenticators stay case-sensitive."""
    credentials = {
        "account": "test_account",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
        "authenticator": "WORKLOAD_IDENTITY",
        "workload_identity_provider": "OIDC",
        "token": "oidc_jwt_token",
        # no 'user' — must not trip validation despite the uppercase authenticator
    }
    with patch("dbt.adapters.snowflake.connections.warn_or_error") as mock_warn:
        creds = connections.SnowflakeCredentials(**credentials)
        mock_warn.assert_not_called()
    auth_args = creds.auth_args()
    assert auth_args["authenticator"] == "WORKLOAD_IDENTITY"
    assert auth_args["token"] == "oidc_jwt_token"


def test_connnections_credentials_wif_authenticator_fails_without_provider():
    credentials = {
        "account": "account_id_with_underscores",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
        "authenticator": "workload_identity",
        # Missing workload_identity_provider
    }
    with pytest.raises(DbtConfigError) as excinfo:
        connections.SnowflakeCredentials(**credentials).auth_args()
    assert (
        "workload_identity_provider must be set to one of the following values if authenticator='workload_identity'!"
        in str(excinfo)
    )


def test_connnections_credentials_wif_authenticator_fails_with_invalid_provider():
    credentials = {
        "account": "account_id_with_underscores",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
        "authenticator": "workload_identity",
        "workload_identity_provider": "some_non_existent_cloud_provider",
    }
    with pytest.raises(DbtConfigError) as excinfo:
        connections.SnowflakeCredentials(**credentials).auth_args()
    assert (
        "workload_identity_provider must be set to one of the following values if authenticator='workload_identity'!"
        in str(excinfo)
    )


def test_connnections_credentials_wif_authenticator_fails_with_entra_resource_and_non_azure_provider():
    credentials = {
        "account": "account_id_with_underscores",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
        "authenticator": "workload_identity",
        "workload_identity_provider": "aws",
        "workload_identity_entra_resource": "app://123",
    }
    with pytest.raises(DbtConfigError) as excinfo:
        connections.SnowflakeCredentials(**credentials).auth_args()
    assert (
        "workload_identity_entra_resource can only be set if workload_identity_provider is Azure"
        in str(excinfo)
    )
