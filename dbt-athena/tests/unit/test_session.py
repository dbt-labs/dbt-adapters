from unittest.mock import Mock, patch, MagicMock
from uuid import UUID

import botocore.session
import pytest
from botocore.exceptions import ClientError
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena import AthenaCredentials
from dbt.adapters.athena.session import (
    AthenaSparkSessionManager,
    _assume_role_session,
    get_boto3_session,
    get_boto3_session_from_credentials,
)
from dbt.adapters.contracts.connection import Connection


class TestSession:
    @pytest.mark.parametrize(
        ("credentials_profile_name", "boto_profile_name"),
        (
            pytest.param(None, "default", id="no_profile_in_credentials"),
            pytest.param("my_profile", "my_profile", id="profile_in_credentials"),
        ),
    )
    def test_session_should_be_called_with_correct_parameters(
        self, monkeypatch, credentials_profile_name, boto_profile_name
    ):
        def mock___build_profile_map(_):
            return {
                **{"default": {}},
                **({} if not credentials_profile_name else {credentials_profile_name: {}}),
            }

        monkeypatch.setattr(
            botocore.session.Session, "_build_profile_map", mock___build_profile_map
        )
        connection = Connection(
            type="test",
            name="test_session",
            credentials=AthenaCredentials(
                database="db",
                schema="schema",
                s3_staging_dir="dir",
                region_name="eu-west-1",
                aws_profile_name=credentials_profile_name,
            ),
        )
        session = get_boto3_session(connection)
        assert session.region_name == "eu-west-1"
        assert session.profile_name == boto_profile_name


class TestAssumeRoleSession:
    STS_RESPONSE = {
        "Credentials": {
            "AccessKeyId": "ASSUMED_ACCESS_KEY",
            "SecretAccessKey": "ASSUMED_SECRET_KEY",
            "SessionToken": "ASSUMED_SESSION_TOKEN",
        }
    }

    def _make_credentials(self, **overrides):
        defaults = dict(
            database="db",
            schema="schema",
            s3_staging_dir="s3://bucket/staging/",
            region_name="ap-northeast-1",
        )
        defaults.update(overrides)
        return AthenaCredentials(**defaults)

    @patch("dbt.adapters.athena.session._assume_role_session")
    def test_no_assume_role_arn_skips_assume_role(self, mock_assume):
        credentials = self._make_credentials()
        connection = Connection(
            type="test",
            name="test_session",
            credentials=credentials,
        )
        get_boto3_session(connection)
        mock_assume.assert_not_called()

    @patch("dbt.adapters.athena.session._assume_role_session")
    def test_no_assume_role_arn_skips_assume_role_from_credentials(self, mock_assume):
        credentials = self._make_credentials()
        get_boto3_session_from_credentials(credentials)
        mock_assume.assert_not_called()

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_assume_role_arn_calls_sts(self, mock_session_cls):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        _assume_role_session(base_session, credentials)

        base_session.client.assert_called_once_with("sts")
        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="dbt-athena",
        )
        mock_session_cls.assert_called_once_with(
            aws_access_key_id="ASSUMED_ACCESS_KEY",
            aws_secret_access_key="ASSUMED_SECRET_KEY",
            aws_session_token="ASSUMED_SESSION_TOKEN",
            region_name="ap-northeast-1",
        )

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_assume_role_with_external_id(self, mock_session_cls):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_external_id="my-external-id",
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        _assume_role_session(base_session, credentials)

        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="dbt-athena",
            ExternalId="my-external-id",
        )

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_assume_role_with_duration_seconds(self, mock_session_cls):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_duration_seconds=3600,
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        _assume_role_session(base_session, credentials)

        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="dbt-athena",
            DurationSeconds=3600,
        )

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_assume_role_with_custom_session_name(self, mock_session_cls):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_session_name="my-custom-session",
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        _assume_role_session(base_session, credentials)

        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="my-custom-session",
        )

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    @patch("dbt.adapters.athena.session._assume_role_session")
    def test_get_boto3_session_calls_assume_role(self, mock_assume, mock_session_cls):
        mock_base = MagicMock()
        mock_session_cls.return_value = mock_base
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        connection = Connection(
            type="test",
            name="test_session",
            credentials=credentials,
        )

        get_boto3_session(connection)

        mock_assume.assert_called_once_with(mock_base, credentials)

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    @patch("dbt.adapters.athena.session._assume_role_session")
    def test_get_boto3_session_from_credentials_calls_assume_role(self, mock_assume, mock_session_cls):
        mock_base = MagicMock()
        mock_session_cls.return_value = mock_base
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )

        get_boto3_session_from_credentials(credentials)

        mock_assume.assert_called_once_with(mock_base, credentials)

    def test_assume_role_sts_error_raises_dbt_runtime_error(self):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Not authorized"}},
            "AssumeRole",
        )
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        with pytest.raises(DbtRuntimeError, match="Failed to assume role"):
            _assume_role_session(base_session, credentials)

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param(899, id="below_minimum"),
            pytest.param(43201, id="above_maximum"),
        ],
    )
    def test_assume_role_invalid_duration_raises_error(self, duration):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_duration_seconds=duration,
        )
        base_session = MagicMock()

        with pytest.raises(DbtRuntimeError, match="assume_role_duration_seconds must be between"):
            _assume_role_session(base_session, credentials)

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_assume_role_with_all_options(self, mock_session_cls):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_external_id="ext-id",
            assume_role_session_name="custom-session",
            assume_role_duration_seconds=3600,
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        _assume_role_session(base_session, credentials)

        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="custom-session",
            ExternalId="ext-id",
            DurationSeconds=3600,
        )


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestAthenaSparkSessionManager:
    """
    A class to test the AthenaSparkSessionManager
    """

    @pytest.fixture
    def spark_session_manager(self, athena_credentials, athena_client, monkeypatch):
        """
        Fixture for creating a mock Spark session manager.

        This fixture creates an instance of AthenaSparkSessionManager with the provided Athena credentials,
        timeout, polling interval, and engine configuration. It then patches the Athena client of the manager
        with the provided `athena_client` object. The fixture returns the mock Spark session manager.

        Args:
            self: The test class instance.
            athena_credentials: The Athena credentials.
            athena_client: The Athena client object.
            monkeypatch: The monkeypatch object for mocking.

        Returns:
            The mock Spark session manager.

        """
        mock_session_manager = AthenaSparkSessionManager(
            athena_credentials,
            timeout=10,
            polling_interval=5,
            engine_config={
                "CoordinatorDpuSize": 1,
                "MaxConcurrentDpus": 2,
                "DefaultExecutorDpuSize": 1,
            },
        )
        monkeypatch.setattr(mock_session_manager, "athena_client", athena_client)
        return mock_session_manager

    @pytest.mark.parametrize(
        "session_status_response, expected_response",
        [
            pytest.param(
                {"Status": {"SessionId": "test_session_id", "State": "CREATING"}},
                DbtRuntimeError(
                    """Session <MagicMock name='client.start_session().__getitem__()' id='140219810489792'>
                    did not create within 10 seconds."""
                ),
                marks=pytest.mark.xfail,
            ),
            (
                {"Status": {"SessionId": "635c1c6d-766c-408b-8bce-fae8ea7006f7", "State": "IDLE"}},
                UUID("635c1c6d-766c-408b-8bce-fae8ea7006f7"),
            ),
            pytest.param(
                {"Status": {"SessionId": "test_session_id", "State": "TERMINATED"}},
                DbtRuntimeError(
                    "Unable to create session: test_session_id. Got status: TERMINATED."
                ),
                marks=pytest.mark.xfail,
            ),
        ],
    )
    def test_start_session(
        self, session_status_response, expected_response, spark_session_manager, athena_client
    ) -> None:
        """
        Test the start_session method of the AthenaJobHelper class.

        Args:
            session_status_response (dict): A dictionary containing the response from the Athena session
            creation status.
            expected_response (Union[dict, DbtRuntimeError]): The expected response from the start_session method.
            athena_job_helper (AthenaPythonJobHelper): An instance of the AthenaPythonJobHelper class.
            athena_client (botocore.client.BaseClient): An instance of the botocore Athena client.

        Returns:
            None
        """
        with patch.multiple(
            spark_session_manager,
            poll_until_session_creation=Mock(return_value=session_status_response),
        ):
            with patch.multiple(
                athena_client,
                get_session_status=Mock(return_value=session_status_response),
                start_session=Mock(return_value=session_status_response.get("Status")),
            ):
                response = spark_session_manager.start_session()
                assert response == expected_response

    @pytest.mark.parametrize(
        "session_status_response, expected_status",
        [
            (
                {
                    "SessionId": "test_session_id",
                    "Status": {
                        "State": "CREATING",
                    },
                },
                {
                    "State": "CREATING",
                },
            ),
            (
                {
                    "SessionId": "test_session_id",
                    "Status": {
                        "State": "IDLE",
                    },
                },
                {
                    "State": "IDLE",
                },
            ),
        ],
    )
    def test_get_session_status(
        self, session_status_response, expected_status, spark_session_manager, athena_client
    ):
        """
        Test the get_session_status function.

        Args:
            self: The test class instance.
            session_status_response (dict): The response from get_session_status.
            expected_status (dict): The expected session status.
            spark_session_manager: The Spark session manager object.
            athena_client: The Athena client object.

        Returns:
            None

        Raises:
            AssertionError: If the retrieved session status is not correct.
        """
        with patch.multiple(
            athena_client, get_session_status=Mock(return_value=session_status_response)
        ):
            response = spark_session_manager.get_session_status("test_session_id")
            assert response == expected_status

    def test_get_session_id(self):
        pass
