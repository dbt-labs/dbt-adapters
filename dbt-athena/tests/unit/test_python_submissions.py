import sys
import time
import uuid
from unittest.mock import MagicMock, Mock, patch

import botocore.exceptions
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.adapters.athena.session import AthenaSparkSessionManager
from dbt.adapters.athena.spark_connect_job import SparkConnectSubmitter
from dbt.adapters.athena.spark_connect_session import SparkConnectSessionPool

from .constants import DATABASE_NAME


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestAthenaPythonJobHelper:
    """
    A class to test the AthenaPythonJobHelper
    """

    @pytest.fixture
    def parsed_model(self, request):
        config: dict[str, int] = request.param.get("config", {"timeout": 1, "polling_interval": 5})

        return {
            "alias": "test_model",
            "schema": DATABASE_NAME,
            "config": {
                "timeout": config["timeout"],
                "polling_interval": config["polling_interval"],
                "engine_config": request.param.get(
                    "engine_config",
                    {
                        "CoordinatorDpuSize": 1,
                        "MaxConcurrentDpus": 2,
                        "DefaultExecutorDpuSize": 1,
                    },
                ),
            },
        }

    @pytest.fixture
    def athena_spark_session_manager(self, athena_credentials, parsed_model):
        return AthenaSparkSessionManager(
            athena_credentials,
            timeout=parsed_model["config"]["timeout"],
            polling_interval=parsed_model["config"]["polling_interval"],
            engine_config=parsed_model["config"]["engine_config"],
        )

    @pytest.fixture
    def athena_job_helper(
        self,
        athena_credentials,
        athena_client,
        athena_spark_session_manager,
        parsed_model,
        monkeypatch,
    ):
        mock_job_helper = AthenaPythonJobHelper(parsed_model, athena_credentials)
        monkeypatch.setattr(mock_job_helper, "athena_client", athena_client)
        monkeypatch.setattr(mock_job_helper, "spark_connection", athena_spark_session_manager)
        return mock_job_helper

    @pytest.mark.parametrize(
        "parsed_model, session_status_response, expected_response",
        [
            (
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "IDLE",
                },
                None,
            ),
            pytest.param(
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "FAILED",
                },
                None,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "TERMINATED",
                },
                None,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "State": "CREATING",
                },
                None,
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_poll_session_idle(
        self,
        session_status_response,
        expected_response,
        athena_job_helper,
        athena_spark_session_manager,
        monkeypatch,
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_status=Mock(return_value=session_status_response),
            get_session_id=Mock(return_value="test_session_id"),
        ):

            def mock_sleep(_):
                pass

            monkeypatch.setattr(time, "sleep", mock_sleep)
            poll_response = athena_job_helper.poll_until_session_idle()
            assert poll_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, execution_status, expected_response",
        [
            (
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "COMPLETED",
                    }
                },
                "COMPLETED",
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "FAILED",
                    }
                },
                None,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "RUNNING",
                    }
                },
                "RUNNING",
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_poll_execution(
        self,
        execution_status,
        expected_response,
        athena_job_helper,
        athena_spark_session_manager,
        athena_client,
        monkeypatch,
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_id=Mock(return_value=uuid.uuid4()),
        ):
            with patch.multiple(
                athena_client,
                get_calculation_execution=Mock(return_value=execution_status),
            ):

                def mock_sleep(_):
                    pass

                monkeypatch.setattr(time, "sleep", mock_sleep)
                poll_response = athena_job_helper.poll_until_execution_completion(
                    "test_calculation_id"
                )
                assert poll_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, test_calculation_execution_id, test_calculation_execution",
        [
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {"CalculationExecutionId": "test_execution_id"},
                {
                    "Result": {"ResultS3Uri": "test_results_s3_uri"},
                    "Status": {"State": "COMPLETED"},
                },
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {"CalculationExecutionId": "test_execution_id"},
                {"Result": {}, "Status": {"State": "FAILED"}},
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {},
                {"Result": {}, "Status": {"State": "FAILED"}},
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_submission(
        self,
        test_calculation_execution_id,
        test_calculation_execution,
        athena_job_helper,
        athena_spark_session_manager,
        athena_client,
    ):
        with patch.multiple(
            athena_spark_session_manager, get_session_id=Mock(return_value=uuid.uuid4())
        ):
            with patch.multiple(
                athena_client,
                start_calculation_execution=Mock(return_value=test_calculation_execution_id),
                get_calculation_execution=Mock(return_value=test_calculation_execution),
            ):
                with patch.multiple(
                    athena_job_helper, poll_until_session_idle=Mock(return_value="IDLE")
                ):
                    result = athena_job_helper.submit("hello world")
                    assert result == test_calculation_execution["Result"]


class TestSessionStateErrorHandling:
    """
    Self-contained tests for session state error handling in submit().
    These tests don't depend on the class-scoped fixtures from conftest.py.
    """

    @pytest.fixture
    def mock_credentials(self):
        """Create a mock credentials object."""
        credentials = Mock()
        credentials.aws_access_key_id = None
        credentials.aws_secret_access_key = None
        credentials.aws_session_token = None
        credentials.region_name = "us-east-1"
        credentials.aws_profile_name = None
        credentials.spark_work_group = "test-workgroup"
        credentials.poll_interval = 1
        credentials.num_retries = 3
        credentials.effective_num_retries = 3
        return credentials

    @pytest.fixture
    def mock_parsed_model(self):
        """Create a mock parsed model."""
        return {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "engine_config": {
                    "CoordinatorDpuSize": 1,
                    "MaxConcurrentDpus": 2,
                    "DefaultExecutorDpuSize": 1,
                },
            },
        }

    @pytest.mark.parametrize(
        "session_state",
        ["TERMINATED", "TERMINATING", "DEGRADED", "FAILED"],
    )
    def test_submit_handles_terminated_session_states(
        self, mock_credentials, mock_parsed_model, session_state
    ):
        """Test that submit() handles terminated session states by getting a new session."""
        first_session_id = str(uuid.uuid4())
        second_session_id = str(uuid.uuid4())

        # Track session_id calls
        session_id_calls = [0]

        def mock_get_session_id():
            session_id_calls[0] += 1
            if session_id_calls[0] == 1:
                return uuid.UUID(first_session_id)
            return uuid.UUID(second_session_id)

        # First call raises ClientError, second succeeds
        error_response = {
            "Error": {
                "Code": "InvalidRequestException",
                "Message": f"Session is in the {session_state} state",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "StartCalculationExecution")

        start_calc_calls = [0]

        def mock_start_calculation(*args, **kwargs):
            start_calc_calls[0] += 1
            if start_calc_calls[0] == 1:
                raise client_error
            return {"CalculationExecutionId": "test_execution_id"}

        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = mock_get_session_id
            mock_session_manager.remove_terminated_session = Mock()
            mock_session_manager.set_spark_session_load = Mock()
            MockSessionManager.return_value = mock_session_manager

            # Create the helper
            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)

            # Mock the athena_client
            mock_athena_client = Mock()
            mock_athena_client.start_calculation_execution = mock_start_calculation
            mock_athena_client.get_calculation_execution = Mock(
                return_value={
                    "Result": {"ResultS3Uri": "test_results_s3_uri"},
                    "Status": {"State": "COMPLETED"},
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            result = helper.submit("print('hello')")

            # Verify session was cleaned up
            mock_session_manager.remove_terminated_session.assert_called_once_with(
                first_session_id
            )
            # Verify we got a result (meaning retry worked)
            assert result == {
                "ResultS3Uri": "test_results_s3_uri",
                "SparkSessionId": second_session_id,
                "SparkCalculationExecutionId": "test_execution_id",
            }
            # Verify we made two attempts
            assert start_calc_calls[0] == 2

    def test_submit_raises_for_unknown_client_error(self, mock_credentials, mock_parsed_model):
        """Test that submit() raises DbtRuntimeError for unknown ClientErrors."""
        session_id = uuid.uuid4()

        error_response = {
            "Error": {
                "Code": "UnknownException",
                "Message": "Some unexpected error occurred",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "StartCalculationExecution")

        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value=session_id)
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)

            mock_athena_client = Mock()
            mock_athena_client.start_calculation_execution = Mock(side_effect=client_error)
            helper.__dict__["athena_client"] = mock_athena_client

            with pytest.raises(DbtRuntimeError) as exc_info:
                helper.submit("print('hello')")

            assert "Unable to start spark python code execution" in str(exc_info.value)
            assert "ClientError" in str(exc_info.value)

    def test_submit_handles_busy_session_state(self, mock_credentials, mock_parsed_model):
        """Test that submit() continues to poll when session is BUSY."""
        session_id = uuid.uuid4()

        # First call raises BUSY error, second succeeds
        error_response = {
            "Error": {
                "Code": "InvalidRequestException",
                "Message": "Session is in the BUSY state; needs to be IDLE to accept Calculations.",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "StartCalculationExecution")

        call_count = [0]

        def mock_start_calculation(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise client_error
            return {"CalculationExecutionId": "test_execution_id"}

        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value=session_id)
            mock_session_manager.get_session_status = Mock(return_value={"State": "IDLE"})
            mock_session_manager.set_spark_session_load = Mock()
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)

            mock_athena_client = Mock()
            mock_athena_client.start_calculation_execution = mock_start_calculation
            mock_athena_client.get_calculation_execution = Mock(
                return_value={
                    "Result": {"ResultS3Uri": "test_results_s3_uri"},
                    "Status": {"State": "COMPLETED"},
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            result = helper.submit("print('hello')")

            # Verify we got a result
            assert result == {
                "ResultS3Uri": "test_results_s3_uri",
                "SparkSessionId": str(session_id),
                "SparkCalculationExecutionId": "test_execution_id",
            }
            # Verify we made two attempts (once failed with BUSY, then succeeded)
            assert call_count[0] == 2


class TestSparkConnectSubmission:
    """Tests for the Apache Spark 3.5 Spark Connect submission path."""

    @pytest.fixture(autouse=True)
    def _reset_pool_singleton(self):
        SparkConnectSessionPool._reset_for_tests()
        yield
        SparkConnectSessionPool._reset_for_tests()

    @pytest.fixture(autouse=True)
    def _fake_pyspark_modules(self, monkeypatch):
        # The submission path does `from pyspark.sql.connect.session import SparkSession`
        # lazily inside the function.  Injecting mock modules keeps the import
        # resolvable without requiring pyspark as a test dependency.
        for mod in (
            "pyspark",
            "pyspark.sql",
            "pyspark.sql.connect",
            "pyspark.sql.connect.session",
        ):
            monkeypatch.setitem(sys.modules, mod, MagicMock())

    @pytest.fixture
    def mock_credentials(self):
        credentials = Mock()
        credentials.aws_access_key_id = None
        credentials.aws_secret_access_key = None
        credentials.aws_session_token = None
        credentials.region_name = "us-east-1"
        credentials.aws_profile_name = None
        credentials.spark_work_group = "test-workgroup"
        credentials.spark_connect_max_sessions = 2
        credentials.spark_connect_session_concurrency = None
        credentials.poll_interval = 0.01
        credentials.num_retries = 3
        credentials.effective_num_retries = 3
        return credentials

    @pytest.fixture
    def spark_connect_parsed_model(self):
        return {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 5,
                "polling_interval": 0.01,
                "engine_config": {
                    "CoordinatorDpuSize": 1,
                    "MaxConcurrentDpus": 2,
                    "DefaultExecutorDpuSize": 1,
                },
                "spark_engine_version": "3.5",
            },
        }

    @pytest.fixture
    def calculations_parsed_model(self):
        # Same shape but without spark_engine_version, so _is_spark_connect is False.
        return {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 5,
                "polling_interval": 0.01,
                "engine_config": {
                    "CoordinatorDpuSize": 1,
                    "MaxConcurrentDpus": 2,
                    "DefaultExecutorDpuSize": 1,
                },
            },
        }

    def _make_helper(self, parsed_model, credentials):
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            MockSessionManager.return_value = Mock()
            helper = AthenaPythonJobHelper(parsed_model, credentials)
        helper.__dict__["athena_client"] = Mock()
        return helper

    def _make_submitter(self, parsed_model, credentials, mock_pool):
        from dbt.adapters.athena.config import AthenaSparkSessionConfig

        config = AthenaSparkSessionConfig(
            parsed_model["config"],
            polling_interval=credentials.poll_interval,
            retry_attempts=credentials.num_retries,
        )
        submitter = SparkConnectSubmitter(
            athena_client=Mock(),
            credentials=credentials,
            config=config,
            engine_config=config.set_engine_config(),
            timeout=parsed_model["config"]["timeout"],
            polling_interval=parsed_model["config"]["polling_interval"],
            relation_name=parsed_model.get("relation_name"),
        )
        # Bypass the cached_property so the pool is fully mockable.
        submitter.__dict__["_pool"] = mock_pool
        return submitter

    def test_dispatches_to_spark_connect_for_engine_version_35(
        self, mock_credentials, spark_connect_parsed_model
    ):
        helper = self._make_helper(spark_connect_parsed_model, mock_credentials)
        assert helper._is_spark_connect is True

        with patch(
            "dbt.adapters.athena.python_submissions.SparkConnectSubmitter"
        ) as MockSubmitter:
            MockSubmitter.return_value.submit.return_value = {"SparkConnect": True}
            result = helper.submit("spark.sql('SELECT 1')")

        MockSubmitter.return_value.submit.assert_called_once_with("spark.sql('SELECT 1')")
        assert result == {"SparkConnect": True}

    def test_calculations_path_is_used_when_engine_version_not_35(
        self, mock_credentials, calculations_parsed_model
    ):
        helper = self._make_helper(calculations_parsed_model, mock_credentials)
        assert helper._is_spark_connect is False

    def test_empty_code_returns_marker_without_acquiring_session(
        self, mock_credentials, spark_connect_parsed_model
    ):
        mock_pool = Mock()
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        result = submitter.submit("   ")

        assert result == {"SparkConnect": True, "SparkSessionId": None}
        mock_pool.acquire.assert_not_called()

    def test_successful_submission_releases_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        monkeypatch.setattr(
            submitter,
            "_wait_for_endpoint",
            Mock(return_value={"EndpointUrl": "https://x", "AuthToken": "tok"}),
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect_job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )

        result = submitter.submit("x = 1")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-1"}
        mock_pool.acquire.assert_called_once()
        mock_pool.release.assert_called_once_with("sid-1")
        mock_pool.terminate_and_remove.assert_not_called()

    def test_transient_error_retries_with_new_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        monkeypatch.setattr(
            submitter,
            "_wait_for_endpoint",
            Mock(return_value={"EndpointUrl": "https://x", "AuthToken": "tok"}),
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect_job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = Exception("Session not active")
        second_spark = MagicMock()
        fake_session_mod = sys.modules["pyspark.sql.connect.session"]
        fake_session_mod.SparkSession.builder.channelBuilder.return_value.create.side_effect = [
            first_spark,
            second_spark,
        ]

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-2"}
        assert mock_pool.acquire.call_count == 2
        # First session was transient-failed, so it should be terminated.
        mock_pool.terminate_and_remove.assert_called_once_with("sid-1")
        # Second session succeeded, so it should be released.
        mock_pool.release.assert_called_once_with("sid-2")

    def test_non_transient_error_raises_without_retry(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        monkeypatch.setattr(
            submitter,
            "_wait_for_endpoint",
            Mock(return_value={"EndpointUrl": "https://x", "AuthToken": "tok"}),
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect_job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )

        fake_spark = MagicMock()
        fake_spark.run.side_effect = ValueError("boom - not transient")
        fake_session_mod = sys.modules["pyspark.sql.connect.session"]
        fake_session_mod.SparkSession.builder.channelBuilder.return_value.create.return_value = (
            fake_spark
        )

        with pytest.raises(DbtRuntimeError, match="Spark Connect execution failed"):
            submitter.submit("spark.run()")

        assert mock_pool.acquire.call_count == 1
        # Non-transient failures release the session instead of terminating it.
        mock_pool.release.assert_called_once_with("sid-1")
        mock_pool.terminate_and_remove.assert_not_called()

    def test_all_retries_exhausted_raises(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2", "sid-3"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        monkeypatch.setattr(
            submitter,
            "_wait_for_endpoint",
            Mock(return_value={"EndpointUrl": "https://x", "AuthToken": "tok"}),
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect_job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Unable to load credentials")
        fake_session_mod = sys.modules["pyspark.sql.connect.session"]
        fake_session_mod.SparkSession.builder.channelBuilder.return_value.create.return_value = (
            fake_spark
        )

        with pytest.raises(
            DbtRuntimeError, match="Spark Connect execution failed after 3 attempts"
        ):
            submitter.submit("spark.run()")

        # Every transient failure — including the final one — terminates the
        # broken session so later models don't pick it up from the pool.
        assert mock_pool.acquire.call_count == 3
        assert mock_pool.terminate_and_remove.call_count == 3
        mock_pool.release.assert_not_called()

    def test_session_key_varies_with_engine_config(
        self, mock_credentials, spark_connect_parsed_model
    ):
        # Same engine config -> identical fingerprint; different -> different.
        mock_pool = Mock()
        submitter_a = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        other_model = dict(spark_connect_parsed_model)
        other_model["config"] = dict(spark_connect_parsed_model["config"])
        other_model["config"]["engine_config"] = {
            "CoordinatorDpuSize": 4,
            "MaxConcurrentDpus": 8,
            "DefaultExecutorDpuSize": 4,
        }
        submitter_b = self._make_submitter(other_model, mock_credentials, mock_pool)

        assert submitter_a._session_fingerprint != submitter_b._session_fingerprint
        assert submitter_a._session_key != submitter_b._session_key
