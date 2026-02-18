import json
import unittest
from unittest.mock import Mock, MagicMock, patch, PropertyMock

from dbt.adapters.bigquery.credentials import BigQueryConnectionMethod
from dbt.adapters.bigquery.python_submissions import BigFramesHelper


class TestGetServiceAccountFromCredentials(unittest.TestCase):
    """Tests for BigFramesHelper._get_service_account_from_credentials.

    Verifies that service account identity is correctly detected from
    credential object properties, covering: impersonated credentials,
    direct service account credentials, external account credentials
    with SA impersonation, and regular user OAuth credentials.
    """

    def _make_helper(self, credentials, connection_method="oauth"):
        """Create a BigFramesHelper with the given Google credentials, bypassing __init__."""
        helper = object.__new__(BigFramesHelper)
        helper._GoogleCredentials = credentials
        helper._connection_method = connection_method
        return helper

    def test_impersonated_credentials_returns_target_principal(self):
        """ADC with SA impersonation returns _target_principal."""
        from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials

        creds = Mock(spec=ImpersonatedCredentials)
        creds._target_principal = "sa@project.iam.gserviceaccount.com"

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result == "sa@project.iam.gserviceaccount.com"

    def test_impersonated_credentials_empty_target_principal_falls_through(self):
        """Impersonated credentials with empty _target_principal falls through to service_account_email."""
        from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials

        creds = Mock(spec=ImpersonatedCredentials)
        creds._target_principal = ""
        creds.service_account_email = "sa@project.iam.gserviceaccount.com"

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result == "sa@project.iam.gserviceaccount.com"

    def test_impersonated_credentials_no_target_principal_no_sa_email_returns_none(self):
        """Impersonated credentials with no _target_principal and no service_account_email."""
        from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials

        creds = Mock(spec=ImpersonatedCredentials)
        creds._target_principal = None
        creds.service_account_email = None

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result is None

    def test_service_account_email_attribute(self):
        """Direct SA credentials (e.g., Compute Engine default SA via ADC)."""
        creds = Mock()
        creds.service_account_email = "compute-sa@project.iam.gserviceaccount.com"

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result == "compute-sa@project.iam.gserviceaccount.com"

    def test_external_account_with_impersonation_url(self):
        """WIF credentials with service_account_impersonation_url."""
        creds = Mock(spec=[])  # no attributes by default
        creds.service_account_impersonation_url = (
            "https://iamcredentials.googleapis.com/v1/projects/-/"
            "serviceAccounts/wif-sa@project.iam.gserviceaccount.com:generateAccessToken"
        )

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result == "wif-sa@project.iam.gserviceaccount.com"

    def test_external_account_impersonation_url_no_match(self):
        """WIF credentials with malformed impersonation URL."""
        creds = Mock(spec=[])
        creds.service_account_impersonation_url = "https://example.com/bad-url"

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result is None

    def test_regular_user_oauth_returns_none(self):
        """Regular user OAuth credentials (no SA properties)."""
        from google.oauth2.credentials import Credentials as GoogleCredentials

        creds = Mock(spec=GoogleCredentials)

        helper = self._make_helper(creds)
        result = helper._get_service_account_from_credentials()

        assert result is None


class TestConfigNotebookJob(unittest.TestCase):
    """Tests for BigFramesHelper._config_notebook_job.

    Verifies the correct field (service_account vs execution_user) is set
    on the NotebookExecutionJob for different credential and auth scenarios.
    """

    def _make_helper(self, credentials, connection_method):
        """Create a BigFramesHelper with given params, bypassing __init__."""
        helper = object.__new__(BigFramesHelper)
        helper._GoogleCredentials = credentials
        helper._connection_method = connection_method
        helper._project = "test-project"
        helper._region = "us-central1"
        helper._gcs_path = "gs://bucket/schema/model.py"
        helper._gcs_bucket = "bucket"
        helper._model_file_name = "schema/model.py"
        helper._parsed_model = {"config": {}, "alias": "model"}
        return helper

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_service_account_method_uses_sa_email(self, _):
        """SERVICE_ACCOUNT method uses _service_account_email directly."""
        from google.cloud import aiplatform_v1

        creds = Mock()
        creds._service_account_email = "direct-sa@project.iam.gserviceaccount.com"

        helper = self._make_helper(creds, BigQueryConnectionMethod.SERVICE_ACCOUNT)
        job = helper._config_notebook_job("template-123")

        assert job.service_account == "direct-sa@project.iam.gserviceaccount.com"
        assert not job.execution_user

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_oauth_with_impersonated_credentials_sets_service_account(self, _):
        """OAuth with ADC SA impersonation sets service_account field."""
        from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials
        from google.cloud import aiplatform_v1

        creds = Mock(spec=ImpersonatedCredentials)
        creds._target_principal = "impersonated-sa@project.iam.gserviceaccount.com"

        helper = self._make_helper(creds, BigQueryConnectionMethod.OAUTH)
        job = helper._config_notebook_job("template-123")

        assert job.service_account == "impersonated-sa@project.iam.gserviceaccount.com"
        assert not job.execution_user

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_oauth_with_compute_engine_sa_sets_service_account(self, _):
        """OAuth with ADC Compute Engine SA sets service_account field."""
        from google.cloud import aiplatform_v1

        creds = Mock()
        creds.service_account_email = "compute-sa@project.iam.gserviceaccount.com"

        helper = self._make_helper(creds, BigQueryConnectionMethod.OAUTH)
        job = helper._config_notebook_job("template-123")

        assert job.service_account == "compute-sa@project.iam.gserviceaccount.com"
        assert not job.execution_user

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_oauth_with_regular_user_sets_execution_user(self, _):
        """OAuth with regular user credentials sets execution_user via userinfo."""
        from google.oauth2.credentials import Credentials as GoogleCredentials
        from google.cloud import aiplatform_v1

        creds = Mock(spec=GoogleCredentials)
        creds.token = "fake-token"

        helper = self._make_helper(creds, BigQueryConnectionMethod.OAUTH)

        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = json.dumps({"email": "user@example.com"}).encode()

        with patch("dbt.adapters.bigquery.python_submissions.Request") as MockRequest:
            mock_request_instance = MockRequest.return_value
            mock_request_instance.return_value = mock_response
            job = helper._config_notebook_job("template-123")

        assert job.execution_user == "user@example.com"
        assert not job.service_account

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_external_oauth_wif_with_impersonation_url_sets_service_account(self, _):
        """EXTERNAL_OAUTH_WIF with SA impersonation URL sets service_account."""
        from google.cloud import aiplatform_v1

        creds = Mock(spec=[])
        creds.service_account_impersonation_url = (
            "https://iamcredentials.googleapis.com/v1/projects/-/"
            "serviceAccounts/wif-sa@project.iam.gserviceaccount.com:generateAccessToken"
        )

        helper = self._make_helper(creds, BigQueryConnectionMethod.EXTERNAL_OAUTH_WIF)
        job = helper._config_notebook_job("template-123")

        assert job.service_account == "wif-sa@project.iam.gserviceaccount.com"
        assert not job.execution_user

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_unsupported_method_raises_error(self, _):
        """Unsupported connection method raises ValueError."""
        from google.cloud import aiplatform_v1

        creds = Mock()
        helper = self._make_helper(creds, "unsupported-method")

        with self.assertRaises(ValueError) as ctx:
            helper._config_notebook_job("template-123")

        assert "Unsupported credential method" in str(ctx.exception)

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_oauth_userinfo_failure_raises_error(self, _):
        """OAuth userinfo endpoint failure raises DbtRuntimeError."""
        from google.oauth2.credentials import Credentials as GoogleCredentials
        from google.cloud import aiplatform_v1
        from dbt_common.exceptions import DbtRuntimeError

        creds = Mock(spec=GoogleCredentials)
        creds.token = "fake-token"

        helper = self._make_helper(creds, BigQueryConnectionMethod.OAUTH)

        mock_response = Mock()
        mock_response.status = 401
        mock_response.data = b"Unauthorized"

        with patch("dbt.adapters.bigquery.python_submissions.Request") as MockRequest:
            mock_request_instance = MockRequest.return_value
            mock_request_instance.return_value = mock_response

            with self.assertRaises(DbtRuntimeError):
                helper._config_notebook_job("template-123")

    @patch("dbt.adapters.bigquery.python_submissions.aiplatform_v1", create=True)
    def test_oauth_userinfo_no_email_raises_error(self, _):
        """OAuth userinfo returns no email raises DbtRuntimeError."""
        from google.oauth2.credentials import Credentials as GoogleCredentials
        from google.cloud import aiplatform_v1
        from dbt_common.exceptions import DbtRuntimeError

        creds = Mock(spec=GoogleCredentials)
        creds.token = "fake-token"

        helper = self._make_helper(creds, BigQueryConnectionMethod.OAUTH)

        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = json.dumps({}).encode()

        with patch("dbt.adapters.bigquery.python_submissions.Request") as MockRequest:
            mock_request_instance = MockRequest.return_value
            mock_request_instance.return_value = mock_response

            with self.assertRaises(DbtRuntimeError):
                helper._config_notebook_job("template-123")
