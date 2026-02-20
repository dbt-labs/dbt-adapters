from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
from uuid import UUID

import botocore.session
import pytest
from botocore.exceptions import ClientError
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena import AthenaCredentials
from dbt.adapters.athena.session import (
    AthenaSparkSessionManager,
    _EXPIRY_BUFFER_SECONDS,
    _get_assume_role_session,
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
    FUTURE_EXPIRATION = datetime.now(timezone.utc) + timedelta(hours=1)
    STS_RESPONSE = {
        "Credentials": {
            "AccessKeyId": "ASSUMED_ACCESS_KEY",
            "SecretAccessKey": "ASSUMED_SECRET_KEY",
            "SessionToken": "ASSUMED_SESSION_TOKEN",
            "Expiration": FUTURE_EXPIRATION,
        }
    }

    @pytest.fixture(autouse=True)
    def clear_assume_role_cache(self):
        _get_assume_role_session.cache_clear()
        yield
        _get_assume_role_session.cache_clear()

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
            DurationSeconds=3600,
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
            DurationSeconds=3600,
        )

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    @patch("dbt.adapters.athena.session._assume_role_session")
    def test_get_boto3_session_calls_assume_role(self, mock_assume, mock_session_cls):
        mock_base = MagicMock()
        mock_assumed = MagicMock()
        mock_session_cls.return_value = mock_base
        mock_assume.return_value = mock_assumed
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        connection = Connection(
            type="test",
            name="test_session",
            credentials=credentials,
        )

        result = get_boto3_session(connection)

        mock_assume.assert_called_once_with(mock_base, credentials)
        assert result is mock_assumed

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    @patch("dbt.adapters.athena.session._assume_role_session")
    def test_get_boto3_session_from_credentials_calls_assume_role(self, mock_assume, mock_session_cls):
        mock_base = MagicMock()
        mock_assumed = MagicMock()
        mock_session_cls.return_value = mock_base
        mock_assume.return_value = mock_assumed
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )

        result = get_boto3_session_from_credentials(credentials)

        mock_assume.assert_called_once_with(mock_base, credentials)
        assert result is mock_assumed

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
            pytest.param(900, id="minimum"),
            pytest.param(43200, id="maximum"),
        ],
    )
    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_assume_role_valid_duration_does_not_raise(self, mock_session_cls, duration):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_duration_seconds=duration,
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        _assume_role_session(base_session, credentials)

        mock_sts.assume_role.assert_called_once()

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

    @patch("dbt.adapters.athena.session.time")
    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_cached_session_is_reused(self, mock_session_cls, mock_time):
        mock_time.time.return_value = 0.0
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        session1 = _assume_role_session(base_session, credentials)
        session2 = _assume_role_session(base_session, credentials)

        assert session1 is session2
        mock_sts.assume_role.assert_called_once()

    @patch("dbt.adapters.athena.session.time")
    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_expired_session_is_refreshed(self, mock_session_cls, mock_time):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        ttl = credentials.assume_role_duration_seconds - _EXPIRY_BUFFER_SECONDS

        session_old = MagicMock(name="old_session")
        session_new = MagicMock(name="new_session")
        mock_session_cls.side_effect = [session_old, session_new]

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        mock_time.time.return_value = 0.0  # _ttl_hash = 0
        session1 = _assume_role_session(base_session, credentials)
        mock_time.time.return_value = float(ttl)  # _ttl_hash = 1 → cache miss
        session2 = _assume_role_session(base_session, credentials)

        assert session1 is session_old
        assert session2 is session_new
        assert mock_sts.assume_role.call_count == 2

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_different_role_arns_use_separate_cache_entries(self, mock_session_cls):
        creds_a = self._make_credentials(
            assume_role_arn="arn:aws:iam::111111111111:role/RoleA",
        )
        creds_b = self._make_credentials(
            assume_role_arn="arn:aws:iam::222222222222:role/RoleB",
        )
        session_a_mock = MagicMock(name="session_a")
        session_b_mock = MagicMock(name="session_b")
        mock_session_cls.side_effect = [session_a_mock, session_b_mock]

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        session_a = _assume_role_session(base_session, creds_a)
        session_b = _assume_role_session(base_session, creds_b)

        assert session_a is session_a_mock
        assert session_b is session_b_mock
        assert mock_sts.assume_role.call_count == 2

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_different_base_sessions_use_separate_cache_entries(self, mock_session_cls):
        credentials = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        session_a_mock = MagicMock(name="session_a")
        session_b_mock = MagicMock(name="session_b")
        mock_session_cls.side_effect = [session_a_mock, session_b_mock]

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session_1 = MagicMock()
        base_session_1.client.return_value = mock_sts
        base_session_2 = MagicMock()
        base_session_2.client.return_value = mock_sts

        session_a = _assume_role_session(base_session_1, credentials)
        session_b = _assume_role_session(base_session_2, credentials)

        assert session_a is session_a_mock
        assert session_b is session_b_mock
        assert mock_sts.assume_role.call_count == 2

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_different_regions_use_separate_cache_entries(self, mock_session_cls):
        creds_a = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            region_name="ap-northeast-1",
        )
        creds_b = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            region_name="us-east-1",
        )
        session_a_mock = MagicMock(name="session_a")
        session_b_mock = MagicMock(name="session_b")
        mock_session_cls.side_effect = [session_a_mock, session_b_mock]

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        session_a = _assume_role_session(base_session, creds_a)
        session_b = _assume_role_session(base_session, creds_b)

        assert session_a is session_a_mock
        assert session_b is session_b_mock
        assert mock_sts.assume_role.call_count == 2

    @patch("dbt.adapters.athena.session.boto3.session.Session")
    def test_different_external_ids_use_separate_cache_entries(self, mock_session_cls):
        creds_a = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_external_id="ext-id-a",
        )
        creds_b = self._make_credentials(
            assume_role_arn="arn:aws:iam::123456789012:role/TestRole",
            assume_role_external_id="ext-id-b",
        )
        session_a_mock = MagicMock(name="session_a")
        session_b_mock = MagicMock(name="session_b")
        mock_session_cls.side_effect = [session_a_mock, session_b_mock]

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = self.STS_RESPONSE
        base_session = MagicMock()
        base_session.client.return_value = mock_sts

        session_a = _assume_role_session(base_session, creds_a)
        session_b = _assume_role_session(base_session, creds_b)

        assert session_a is session_a_mock
        assert session_b is session_b_mock
        assert mock_sts.assume_role.call_count == 2


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
