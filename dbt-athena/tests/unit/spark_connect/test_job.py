"""Tests for the Spark Connect submission path (Apache Spark 3.5+)."""

import sys
import time
from unittest.mock import MagicMock, Mock, patch

import botocore.exceptions
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.adapters.athena.spark_connect.job import SparkConnectSubmitter
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool


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
        # Same shape but without spark_engine_version, so is_spark_connect is False.
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

    def _stub_endpoint_and_channel(self, submitter, monkeypatch):
        """Stub the endpoint wait and channel builder so submit() reaches the pyspark layer."""
        monkeypatch.setattr(
            submitter,
            "_wait_for_endpoint",
            Mock(return_value={"EndpointUrl": "https://x", "AuthToken": "tok"}),
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )

    def _set_spark_create(self, *, return_value=None, side_effect=None):
        """Wire the fake ``SparkSession.builder.channelBuilder(...).create()`` chain."""
        create_mock = sys.modules[
            "pyspark.sql.connect.session"
        ].SparkSession.builder.channelBuilder.return_value.create
        if side_effect is not None:
            create_mock.side_effect = side_effect
        if return_value is not None:
            create_mock.return_value = return_value

    def test_dispatches_to_spark_connect_for_engine_version_35(
        self, mock_credentials, spark_connect_parsed_model
    ):
        helper = self._make_helper(spark_connect_parsed_model, mock_credentials)
        assert helper.config.is_spark_connect is True

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
        assert helper.config.is_spark_connect is False

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
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        result = submitter.submit("x = 1")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-1"}
        mock_pool.acquire.assert_called_once()
        mock_pool.release.assert_called_once_with("sid-1")
        mock_pool.terminate.assert_not_called()

    def test_transient_error_retries_with_new_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = Exception("Session not active")
        second_spark = MagicMock()
        self._set_spark_create(side_effect=[first_spark, second_spark])

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-2"}
        assert mock_pool.acquire.call_count == 2
        # First session was transient-failed, so it should be terminated.
        mock_pool.terminate.assert_called_once_with("sid-1")
        # Second session succeeded, so it should be released.
        mock_pool.release.assert_called_once_with("sid-2")

    def test_non_transient_error_raises_without_retry(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = ValueError("boom - not transient")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(DbtRuntimeError, match="Spark Connect execution failed"):
            submitter.submit("spark.run()")

        assert mock_pool.acquire.call_count == 1
        # Non-transient failures release the session instead of terminating it.
        mock_pool.release.assert_called_once_with("sid-1")
        mock_pool.terminate.assert_not_called()

    def test_all_retries_exhausted_raises(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2", "sid-3"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Unable to load credentials")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(
            DbtRuntimeError, match="Spark Connect execution failed after 3 attempts"
        ):
            submitter.submit("spark.run()")

        # Every transient failure — including the final one — terminates the
        # broken session so later models don't pick it up from the pool.
        assert mock_pool.acquire.call_count == 3
        assert mock_pool.terminate.call_count == 3
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

    def test_session_fingerprint_is_stable_for_identical_config(
        self, mock_credentials, spark_connect_parsed_model
    ):
        mock_pool = Mock()
        submitter_a = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        submitter_b = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        assert submitter_a._session_fingerprint == submitter_b._session_fingerprint
        assert submitter_a._session_key == submitter_b._session_key

    def test_watchdog_interrupts_and_raises_when_timer_fires(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("interrupted by watchdog")
        self._set_spark_create(return_value=fake_spark)

        # Force the watchdog to fire synchronously so timeout_event is set
        # before exec() raises; the except branch then turns this into a
        # timeout error rather than a transient retry.
        class _ImmediateTimer:
            def __init__(self, interval, function):
                self._fn = function

            def start(self):
                self._fn()

            def cancel(self):
                pass

            def join(self, timeout=None):
                pass

        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.threading.Timer",
            _ImmediateTimer,
        )

        with pytest.raises(DbtRuntimeError, match="timed out after"):
            submitter.submit("spark.run()")

        fake_spark.interruptAll.assert_called()

    def test_watchdog_does_not_interrupt_on_successful_execution(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = Mock()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        self._set_spark_create(return_value=fake_spark)

        submitter.submit("x = 1")

        fake_spark.interruptAll.assert_not_called()

    def test_retry_loop_aborts_when_budget_below_backoff(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        # timeout=1s ensures the 2s backoff after attempt 1 exceeds remaining
        # budget, so the loop gives up before attempt 2.
        short_budget_model = dict(spark_connect_parsed_model)
        short_budget_model["config"] = dict(spark_connect_parsed_model["config"])
        short_budget_model["config"]["timeout"] = 1

        mock_pool = Mock()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(short_budget_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(DbtRuntimeError, match="failed after 3 attempts"):
            submitter.submit("spark.run()")

        # Only the first attempt actually ran; backoff guard skipped attempts 2-3.
        assert mock_pool.acquire.call_count == 1
        mock_pool.terminate.assert_called_once_with("sid-1")


class TestWaitForEndpoint:
    """Direct tests for ``SparkConnectSubmitter._wait_for_endpoint``."""

    @pytest.fixture(autouse=True)
    def _no_real_sleep(self, monkeypatch):
        # tenacity reads ``time.sleep`` from ``tenacity.nap`` at module load,
        # so patching the namespace import there is the only place that
        # actually short-circuits the retry sleeps.
        monkeypatch.setattr("tenacity.nap.time.sleep", lambda *_: None)

    def _make_submitter(self, athena_client, polling_interval=0.01):
        submitter = SparkConnectSubmitter.__new__(SparkConnectSubmitter)
        submitter.athena_client = athena_client
        submitter.polling_interval = polling_interval
        return submitter

    def _client_error(self, code):
        return botocore.exceptions.ClientError(
            error_response={"Error": {"Code": code, "Message": code}},
            operation_name="GetSessionEndpoint",
        )

    def test_returns_response_when_endpoint_and_token_present(self):
        client = Mock()
        client.get_session_endpoint.return_value = {
            "EndpointUrl": "https://x",
            "AuthToken": "tok",
        }
        submitter = self._make_submitter(client)

        response = submitter._wait_for_endpoint("sid", remaining_budget=10)

        assert response == {"EndpointUrl": "https://x", "AuthToken": "tok"}
        assert client.get_session_endpoint.call_count == 1

    def test_retries_when_endpoint_url_present_but_auth_token_missing(self):
        client = Mock()
        client.get_session_endpoint.side_effect = [
            {"EndpointUrl": "https://x", "AuthToken": None},
            {"EndpointUrl": "https://x", "AuthToken": "tok"},
        ]
        submitter = self._make_submitter(client)

        response = submitter._wait_for_endpoint("sid", remaining_budget=10)

        assert response["AuthToken"] == "tok"
        assert client.get_session_endpoint.call_count == 2

    def test_remaining_budget_caps_wait_time(self):
        client = Mock()
        client.get_session_endpoint.return_value = {"EndpointUrl": None}
        submitter = self._make_submitter(client, polling_interval=0.05)

        with pytest.raises(DbtRuntimeError, match="endpoint did not become ready within 0.1s"):
            submitter._wait_for_endpoint("sid", remaining_budget=0.1)

    def test_endpoint_cap_applies_when_budget_is_larger(self, monkeypatch):
        # Force the cap to a small value so the test runs quickly while still
        # asserting that _ENDPOINT_READY_TIMEOUT_SECONDS bounds the wait.
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job._ENDPOINT_READY_TIMEOUT_SECONDS",
            0.1,
        )
        client = Mock()
        client.get_session_endpoint.return_value = {"EndpointUrl": None}
        submitter = self._make_submitter(client, polling_interval=0.05)

        with pytest.raises(DbtRuntimeError, match="endpoint did not become ready within 0.1s"):
            submitter._wait_for_endpoint("sid", remaining_budget=999)

    def test_throttling_exception_keeps_polling(self):
        client = Mock()
        client.get_session_endpoint.side_effect = [
            self._client_error("ThrottlingException"),
            {"EndpointUrl": "https://x", "AuthToken": "tok"},
        ]
        submitter = self._make_submitter(client)

        response = submitter._wait_for_endpoint("sid", remaining_budget=10)

        assert response["AuthToken"] == "tok"
        assert client.get_session_endpoint.call_count == 2

    def test_non_throttling_client_error_keeps_polling(self):
        client = Mock()
        client.get_session_endpoint.side_effect = [
            self._client_error("ResourceNotFoundException"),
            {"EndpointUrl": "https://x", "AuthToken": "tok"},
        ]
        submitter = self._make_submitter(client)

        response = submitter._wait_for_endpoint("sid", remaining_budget=10)

        assert response["AuthToken"] == "tok"
        assert client.get_session_endpoint.call_count == 2
