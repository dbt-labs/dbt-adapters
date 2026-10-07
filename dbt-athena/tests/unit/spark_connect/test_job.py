"""Tests for the Spark Connect submission path (Apache Spark 3.5+)."""

import os
import re
import sys
import threading
import time
from unittest.mock import MagicMock, Mock, patch

import boto3
import botocore.exceptions
import botocore.session
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.adapters.athena.spark_connect.job import (
    SparkConnectSubmitter,
    _ExecutionGuard,
    _spark_max_executors,
    _TransientAttemptFailure,
)
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
        return AthenaCredentials(
            database="db",
            schema="schema",
            region_name="us-east-1",
            spark_work_group="test-workgroup",
            spark_connect_max_sessions=2,
            poll_interval=0.01,
            num_retries=3,
        )

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

    @staticmethod
    def _mock_pool():
        """Pool mock with no Spark client bound yet.

        ``get_spark`` returns None so the submitter creates a client, and
        ``set_spark`` hands back the client it was given, as the real pool
        does when no other caller bound one first.
        """
        pool = Mock()
        pool.get_spark.return_value = None
        pool.set_spark.side_effect = lambda _sid, spark: spark
        return pool

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
        submitter._pool = mock_pool
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
        mock_pool = self._mock_pool()
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        result = submitter.submit("   ")

        assert result == {"SparkConnect": True, "SparkSessionId": None}
        mock_pool.acquire.assert_not_called()

    def test_successful_submission_releases_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        self._set_spark_create(return_value=fake_spark)

        result = submitter.submit("x = 1")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-1"}
        mock_pool.acquire.assert_called_once()
        mock_pool.release.assert_called_once_with("sid-1")
        mock_pool.terminate.assert_not_called()
        # The client is bound to the Athena session and kept for the next
        # model; stopping it here would leak a server-side Spark Connect
        # session on pyspark 3.5.
        mock_pool.set_spark.assert_called_once_with("sid-1", fake_spark)
        fake_spark.stop.assert_not_called()

    def test_assume_role_installs_assumed_default_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_credentials.assume_role_arn = "arn:aws:iam::123456789012:role/dbt"
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        assumed_session = Mock()
        get_session = Mock(return_value=assumed_session)
        setup_default = Mock()
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.get_boto3_session_from_credentials",
            get_session,
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.boto3.setup_default_session", setup_default
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.boto3.DEFAULT_SESSION", None, raising=False
        )

        submitter.submit("x = 1")

        get_session.assert_called_once_with(mock_credentials)
        assert boto3.DEFAULT_SESSION is assumed_session
        setup_default.assert_not_called()

    def test_assume_role_default_session_survives_s3_client_creation(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        # Regression: re-wrapping the assumed session as the default double-
        # registers boto3's s3 client-class handler, so boto3.client("s3")
        # raises a duplicate upload_file injection.
        mock_credentials.assume_role_arn = "arn:aws:iam::123456789012:role/dbt"
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        base = botocore.session.Session()
        base.set_config_variable("region", "us-east-1")
        assumed_session = boto3.session.Session(
            botocore_session=base,
            aws_access_key_id="AKIA_ASSUMED",
            aws_secret_access_key="SECRET",
            aws_session_token="TOKEN",
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.get_boto3_session_from_credentials",
            Mock(return_value=assumed_session),
        )
        monkeypatch.setattr(boto3, "DEFAULT_SESSION", None, raising=False)

        submitter.submit("x = 1")

        boto3.client("s3", region_name="us-east-1")
        assert boto3.DEFAULT_SESSION is assumed_session

    def test_no_assume_role_leaves_default_session_untouched(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_credentials.assume_role_arn = None
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        get_session = Mock()
        setup_default = Mock()
        sentinel = Mock()
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.get_boto3_session_from_credentials",
            get_session,
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.boto3.setup_default_session", setup_default
        )
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.boto3.DEFAULT_SESSION", sentinel, raising=False
        )

        submitter.submit("x = 1")

        get_session.assert_not_called()
        setup_default.assert_not_called()
        assert boto3.DEFAULT_SESSION is sentinel

    def test_transient_error_retries_with_new_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
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

    @pytest.mark.parametrize("interval, expect_keepalive", [(0.02, True), (0, False)])
    def test_keepalive_runs_only_while_model_executes(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch, interval, expect_keepalive
    ):
        mock_credentials.spark_connect_keepalive_interval = interval
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        fake_spark = MagicMock()
        self._set_spark_create(return_value=fake_spark)

        submitter.submit(
            "import time\n"
            "for _ in range(50):\n"
            "    if spark.sql.call_count:\n"
            "        break\n"
            "    time.sleep(0.01)\n"
        )

        keepalive_calls = fake_spark.sql.call_count
        assert (keepalive_calls > 0) is expect_keepalive
        time.sleep(0.1)
        assert fake_spark.sql.call_count == keepalive_calls

    def test_non_transient_error_raises_without_retry(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
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

    def test_permission_denied_with_dead_session_retries_with_new_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        class _FakeCode:
            name = "PERMISSION_DENIED"

        class _FakeRpcError(Exception):
            def __init__(self):
                super().__init__("Received http2 header with status: 403")

            def code(self):
                return _FakeCode()

        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        mock_pool.is_session_alive.return_value = False
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = _FakeRpcError()
        second_spark = MagicMock()
        self._set_spark_create(side_effect=[first_spark, second_spark])

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-2"}
        assert mock_pool.acquire.call_count == 2
        mock_pool.terminate.assert_called_once_with("sid-1")
        mock_pool.release.assert_called_once_with("sid-2")

    def test_permission_denied_with_dead_session_on_last_attempt_raises_terminated(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        class _FakeCode:
            name = "PERMISSION_DENIED"

        class _FakeRpcError(Exception):
            def __init__(self):
                super().__init__("Received http2 header with status: 403")

            def code(self):
                return _FakeCode()

        mock_credentials.spark_connect_max_retries = 0
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        mock_pool.is_session_alive.return_value = False
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = _FakeRpcError()
        self._set_spark_create(return_value=fake_spark)

        from dbt.adapters.athena.exceptions import SparkSessionTerminatedError

        with pytest.raises(
            SparkSessionTerminatedError, match="Athena terminated Spark session sid-1"
        ) as excinfo:
            submitter.submit("spark.run()")

        # Original 403 must remain reachable via ``raise ... from e``.
        assert excinfo.value.__cause__ is not None
        assert excinfo.value.__cause__.code().name == "PERMISSION_DENIED"

        assert mock_pool.acquire.call_count == 1
        mock_pool.is_session_alive.assert_called_once_with("sid-1")
        mock_pool.terminate.assert_called_once_with("sid-1")
        mock_pool.release.assert_not_called()

    @pytest.mark.parametrize(
        "message",
        [
            "[NO_ACTIVE_SESSION] No active Spark session found. "
            "Please create a new Spark session before running the code.",
            "An error occurred (InvalidRequestException) when calling the "
            "GetSessionEndpoint operation: Can not generate Session endpoint URL "
            "for Session in STOPPED state",
        ],
    )
    def test_session_ended_by_athena_retries_with_new_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch, message
    ):
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = Exception(message)
        second_spark = MagicMock()
        self._set_spark_create(side_effect=[first_spark, second_spark])

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-2"}
        mock_pool.terminate.assert_called_once_with("sid-1")
        mock_pool.release.assert_called_once_with("sid-2")

    @pytest.mark.parametrize("session_alive", [False, True])
    def test_session_ended_message_on_last_attempt_raises_by_session_state(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch, session_alive
    ):
        mock_credentials.spark_connect_max_retries = 0
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        mock_pool.is_session_alive.return_value = session_alive
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception(
            "[NO_ACTIVE_SESSION] No active Spark session found."
        )
        self._set_spark_create(return_value=fake_spark)

        from dbt.adapters.athena.exceptions import SparkSessionTerminatedError

        with pytest.raises(DbtRuntimeError) as excinfo:
            submitter.submit("spark.run()")

        assert isinstance(excinfo.value, SparkSessionTerminatedError) is (not session_alive)
        mock_pool.is_session_alive.assert_called_once_with("sid-1")
        mock_pool.terminate.assert_called_once_with("sid-1")

    def test_session_ended_when_backoff_exceeds_timeout_raises_terminated(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        short_budget_model = dict(spark_connect_parsed_model)
        short_budget_model["config"] = dict(spark_connect_parsed_model["config"])
        short_budget_model["config"]["timeout"] = 1

        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        mock_pool.is_session_alive.return_value = False
        submitter = self._make_submitter(short_budget_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        from dbt.adapters.athena.exceptions import SparkSessionTerminatedError

        with pytest.raises(
            SparkSessionTerminatedError, match="Athena terminated Spark session sid-1"
        ):
            submitter.submit("spark.run()")

        assert mock_pool.acquire.call_count == 1

    def test_session_ended_on_endpoint_lookup_retries_with_new_session(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        stopped = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "InvalidRequestException",
                    "Message": "Can not generate Session endpoint URL for Session in STOPPED state",
                }
            },
            operation_name="GetSessionEndpoint",
        )
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        submitter.athena_client.get_session_endpoint.side_effect = [
            stopped,
            {"EndpointUrl": "https://x", "AuthToken": "tok"},
        ]
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )
        monkeypatch.setattr(time, "sleep", lambda *_: None)
        monkeypatch.setattr("tenacity.nap.time.sleep", lambda *_: None)
        self._set_spark_create(return_value=MagicMock())

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-2"}
        mock_pool.terminate.assert_called_once_with("sid-1")
        mock_pool.release.assert_called_once_with("sid-2")

    @pytest.mark.parametrize(
        "retry_on, message, retried",
        [
            (["session_ended"], "Session not active", True),
            (["session_ended"], "Maximum allowed sessions", False),
            (["capacity"], "Maximum allowed sessions", True),
            (["capacity"], "Unable to load credentials", False),
            (["executor_environment"], "Unable to load credentials", True),
            (["executor_environment"], "Pool not running", False),
            (["connection"], "Pool not running", True),
            (["connection"], "Session not active", False),
            ([], "Session not active", False),
        ],
    )
    def test_retry_on_limits_retried_categories(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch, retry_on, message, retried
    ):
        mock_credentials.spark_connect_retry_on = retry_on
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        mock_pool.is_session_alive.return_value = True
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = Exception(message)
        self._set_spark_create(side_effect=[first_spark, MagicMock()])

        if retried:
            assert submitter.submit("spark.run()")["SparkSessionId"] == "sid-2"
            assert mock_pool.acquire.call_count == 2
        else:
            with pytest.raises(DbtRuntimeError, match="not in spark_connect_retry_on"):
                submitter.submit("spark.run()")
            assert mock_pool.acquire.call_count == 1
        mock_pool.terminate.assert_called_once_with("sid-1")

    @pytest.mark.parametrize("session_alive, retried", [(False, True), (True, False)])
    def test_retry_on_session_ended_uses_session_state_for_permission_denied(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch, session_alive, retried
    ):
        class _FakeCode:
            name = "PERMISSION_DENIED"

        class _FakeRpcError(Exception):
            def code(self):
                return _FakeCode()

        mock_credentials.spark_connect_retry_on = ["session_ended"]
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        mock_pool.is_session_alive.return_value = session_alive
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = _FakeRpcError("Received http2 header with status: 403")
        self._set_spark_create(side_effect=[first_spark, MagicMock()])

        if retried:
            assert submitter.submit("spark.run()")["SparkSessionId"] == "sid-2"
        else:
            with pytest.raises(
                DbtRuntimeError, match="category 'connection' is not in spark_connect_retry_on"
            ):
                submitter.submit("spark.run()")
        assert mock_pool.acquire.call_count == (2 if retried else 1)

    def test_excluded_session_end_raises_terminated_without_retry(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_credentials.spark_connect_retry_on = []
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        mock_pool.is_session_alive.return_value = False
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        from dbt.adapters.athena.exceptions import SparkSessionTerminatedError

        with pytest.raises(SparkSessionTerminatedError, match="sid-1"):
            submitter.submit("spark.run()")
        assert mock_pool.acquire.call_count == 1

    def test_permission_denied_with_live_session_still_retries(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        """403 + healthy session = transient throttling → retry as before."""

        class _FakeCode:
            name = "PERMISSION_DENIED"

        class _FakeRpcError(Exception):
            def __init__(self):
                super().__init__("Received http2 header with status: 403")

            def code(self):
                return _FakeCode()

        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        mock_pool.is_session_alive.return_value = True
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        first_spark = MagicMock()
        first_spark.run.side_effect = _FakeRpcError()
        second_spark = MagicMock()
        self._set_spark_create(side_effect=[first_spark, second_spark])

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-2"}
        assert mock_pool.acquire.call_count == 2
        mock_pool.terminate.assert_called_once_with("sid-1")
        mock_pool.release.assert_called_once_with("sid-2")

    def test_all_retries_exhausted_raises(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        # Per-attempt budget must comfortably exceed the largest backoff so
        # the backoff guard doesn't short-circuit the loop before all retries
        # actually run.
        long_budget_model = dict(spark_connect_parsed_model)
        long_budget_model["config"] = dict(spark_connect_parsed_model["config"])
        long_budget_model["config"]["timeout"] = 60

        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2", "sid-3", "sid-4"]
        submitter = self._make_submitter(long_budget_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Unable to load credentials")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(
            DbtRuntimeError, match="Spark Connect execution failed after 4 attempts"
        ):
            submitter.submit("spark.run()")

        # Every transient failure — including the final one — terminates the
        # broken session so later models don't pick it up from the pool.
        assert mock_pool.acquire.call_count == 4
        assert mock_pool.terminate.call_count == 4
        mock_pool.release.assert_not_called()

    def test_session_key_varies_with_engine_config(
        self, mock_credentials, spark_connect_parsed_model
    ):
        # Same engine config -> identical fingerprint; different -> different.
        mock_pool = self._mock_pool()
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
        mock_pool = self._mock_pool()
        submitter_a = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        submitter_b = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)

        assert submitter_a._session_fingerprint == submitter_b._session_fingerprint
        assert submitter_a._session_key == submitter_b._session_key

    def test_max_retries_is_configurable_via_credentials(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        # Per-attempt budget must comfortably exceed the largest backoff so
        # the backoff guard doesn't short-circuit the loop before all retries
        # actually run.
        long_budget_model = dict(spark_connect_parsed_model)
        long_budget_model["config"] = dict(spark_connect_parsed_model["config"])
        long_budget_model["config"]["timeout"] = 60

        mock_credentials.spark_connect_max_retries = 5
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = [f"sid-{i}" for i in range(1, 7)]
        submitter = self._make_submitter(long_budget_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(
            DbtRuntimeError, match="Spark Connect execution failed after 6 attempts"
        ):
            submitter.submit("spark.run()")

        assert mock_pool.acquire.call_count == 6

    def test_max_retries_zero_runs_single_attempt(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_credentials.spark_connect_max_retries = 0
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(
            DbtRuntimeError, match="Spark Connect execution failed after 1 attempts"
        ):
            submitter.submit("spark.run()")

        assert mock_pool.acquire.call_count == 1

    def test_watchdog_interrupts_and_raises_when_timer_fires(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
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

        fake_spark.interruptTag.assert_called_once()
        (tag,) = fake_spark.interruptTag.call_args.args
        fake_spark.addTag.assert_called_once_with(tag)
        fake_spark.removeTag.assert_called_once_with(tag)
        fake_spark.interruptAll.assert_not_called()

    def test_watchdog_does_not_interrupt_on_successful_execution(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)

        fake_spark = MagicMock()
        self._set_spark_create(return_value=fake_spark)

        submitter.submit("x = 1")

        fake_spark.interruptTag.assert_not_called()
        fake_spark.interruptAll.assert_not_called()

    def test_retry_loop_aborts_when_backoff_exceeds_per_attempt_budget(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        # timeout=1s ensures the 2s backoff after attempt 1 is at least the
        # per-attempt execution budget, so a retry could not make meaningful
        # progress and the loop gives up before attempt 2.
        short_budget_model = dict(spark_connect_parsed_model)
        short_budget_model["config"] = dict(spark_connect_parsed_model["config"])
        short_budget_model["config"]["timeout"] = 1

        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(short_budget_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(DbtRuntimeError, match="failed after 1 attempts"):
            submitter.submit("spark.run()")

        # Only the first attempt actually ran; backoff guard skipped attempts 2-4.
        assert mock_pool.acquire.call_count == 1
        mock_pool.terminate.assert_called_once_with("sid-1")

    def test_retry_attempt_receives_full_timeout_budget(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        """Each retry attempt must receive the full per-attempt timeout.

        Transient failures discard their session's work. Charging the next
        retry for the lost attempt's elapsed time would shrink its budget
        below what it needs to complete, defeating the point of retrying.
        """
        mock_pool = self._mock_pool()
        mock_pool.acquire.side_effect = ["sid-1", "sid-2"]
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        endpoint_calls: list[float] = []

        def _record_endpoint(session_id, remaining_budget):
            endpoint_calls.append(remaining_budget)
            return {"EndpointUrl": "https://x", "AuthToken": "tok"}

        monkeypatch.setattr(submitter, "_wait_for_endpoint", _record_endpoint)
        monkeypatch.setattr(
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
            Mock(return_value=Mock()),
        )

        first_spark = MagicMock()
        first_spark.run.side_effect = Exception("Session not active")
        second_spark = MagicMock()
        self._set_spark_create(side_effect=[first_spark, second_spark])

        submitter.submit("spark.run()")

        # Both attempts received the full per-attempt budget; the first
        # attempt's elapsed time did not eat into the second's.
        timeout = spark_connect_parsed_model["config"]["timeout"]
        assert endpoint_calls == [timeout, timeout]

    def test_reuses_client_bound_to_session_without_new_endpoint_call(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        bound_spark = MagicMock()
        mock_pool.get_spark.return_value = bound_spark
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        create_mock = MagicMock()
        self._set_spark_create(return_value=create_mock)

        result = submitter.submit("spark.run()")

        assert result == {"SparkConnect": True, "SparkSessionId": "sid-1"}
        bound_spark.run.assert_called_once()
        submitter._wait_for_endpoint.assert_not_called()
        mock_pool.set_spark.assert_not_called()
        sys.modules[
            "pyspark.sql.connect.session"
        ].SparkSession.builder.channelBuilder.return_value.create.assert_not_called()
        bound_spark.stop.assert_not_called()
        mock_pool.release.assert_called_once_with("sid-1")

    def test_losing_bind_race_stops_own_client_and_uses_shared_one(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        shared_spark = MagicMock()
        mock_pool.set_spark.side_effect = lambda _sid, _spark: shared_spark
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        created_spark = MagicMock()
        self._set_spark_create(return_value=created_spark)

        submitter.submit("spark.run()")

        created_spark.stop.assert_called_once()
        created_spark.run.assert_not_called()
        shared_spark.run.assert_called_once()
        shared_spark.stop.assert_not_called()

    def test_transient_failure_leaves_client_stop_to_pool_terminate(
        self, mock_credentials, spark_connect_parsed_model, monkeypatch
    ):
        mock_credentials.spark_connect_max_retries = 0
        mock_pool = self._mock_pool()
        mock_pool.acquire.return_value = "sid-1"
        submitter = self._make_submitter(spark_connect_parsed_model, mock_credentials, mock_pool)
        self._stub_endpoint_and_channel(submitter, monkeypatch)
        fake_spark = MagicMock()
        fake_spark.run.side_effect = Exception("Session not active")
        self._set_spark_create(return_value=fake_spark)

        with pytest.raises(DbtRuntimeError):
            submitter.submit("spark.run()")

        # The pool owns the client now: terminate() stops it together with
        # the Athena session, so the submitter must not stop it itself.
        mock_pool.terminate.assert_called_once_with("sid-1")
        fake_spark.stop.assert_not_called()
        fake_spark.removeTag.assert_called_once()


class _FakeRpcCode:
    def __init__(self, name):
        self.name = name


class _FakeRpcError(Exception):
    def __init__(self, message, code_name):
        super().__init__(message)
        self._code = _FakeRpcCode(code_name)

    def code(self):
        return self._code


class TestTransientAttemptFailure:
    @pytest.fixture
    def submitter(self):
        credentials = AthenaCredentials(
            database="db", schema="schema", region_name="us-east-1", spark_work_group="wg"
        )
        submitter = SparkConnectSubmitter(
            athena_client=Mock(),
            credentials=credentials,
            config=Mock(spark_engine_version="3.5"),
            engine_config={"MaxConcurrentDpus": 2},
            timeout=5,
            polling_interval=0.01,
            relation_name="rel",
        )
        submitter._pool = Mock()
        return submitter

    def _classify(self, submitter, error, attempt=1):
        try:
            raise error
        except Exception as e:
            return submitter._classify_failure(e, "sid-1", attempt)

    def test_transient_error_carries_its_classification(self, submitter):
        error = Exception("Pool not running")

        failure = self._classify(submitter, error)

        assert failure.error is error
        assert failure.session_id == "sid-1"
        assert failure.category == "connection"
        assert failure.retryable is True
        assert failure.session_ended is False

    def test_category_outside_retry_on_is_not_retryable(self, submitter):
        submitter.credentials.spark_connect_retry_on = ["capacity"]

        failure = self._classify(submitter, Exception("Pool not running"))

        assert (failure.category, failure.retryable) == ("connection", False)

    def test_dead_session_after_permission_denied_is_session_ended(self, submitter):
        submitter._pool.is_session_alive.return_value = False

        failure = self._classify(submitter, _FakeRpcError("403", "PERMISSION_DENIED"))

        assert (failure.category, failure.session_ended) == ("session_ended", True)
        submitter._pool.is_session_alive.assert_called_once_with("sid-1")

    def test_live_session_after_permission_denied_stays_connection(self, submitter):
        submitter._pool.is_session_alive.return_value = True

        failure = self._classify(submitter, _FakeRpcError("403", "PERMISSION_DENIED"))

        assert (failure.category, failure.session_ended) == ("connection", False)

    def test_unclassified_error_is_not_transient(self, submitter):
        error = self._classify(submitter, ValueError("boom"))

        assert isinstance(error, DbtRuntimeError)
        assert re.search(r"failed \(session sid-1\): ValueError: boom", str(error))

    def test_attempt_raises_failure_and_terminates_session(self, submitter, monkeypatch):
        submitter._pool.acquire.return_value = "sid-1"
        monkeypatch.setattr(submitter, "_get_or_create_spark", Mock(return_value=MagicMock()))

        with pytest.raises(_TransientAttemptFailure) as excinfo:
            submitter._attempt("raise Exception('Pool not running')", 1, time.monotonic())

        assert excinfo.value.session_id == "sid-1"
        assert excinfo.value.category == "connection"
        submitter._pool.terminate.assert_called_once_with("sid-1")
        submitter._pool.release.assert_not_called()

    def test_attempt_classifies_before_guard_cleanup(self, submitter, monkeypatch):
        submitter._pool.acquire.return_value = "sid-1"
        spark = MagicMock()
        monkeypatch.setattr(submitter, "_get_or_create_spark", Mock(return_value=spark))
        calls = []
        spark.removeTag.side_effect = lambda *_: calls.append("removeTag")
        original_failure_for = submitter._failure_for

        def failure_for(*args, **kwargs):
            calls.append("classify")
            return original_failure_for(*args, **kwargs)

        monkeypatch.setattr(submitter, "_failure_for", failure_for)

        with pytest.raises(_TransientAttemptFailure):
            submitter._attempt("raise Exception('Pool not running')", 1, time.monotonic())

        assert calls == ["classify", "removeTag"]

    def test_watchdog_firing_during_cleanup_does_not_turn_failure_into_timeout(
        self, submitter, monkeypatch
    ):
        submitter._pool.acquire.return_value = "sid-1"
        monkeypatch.setattr(submitter, "_get_or_create_spark", Mock(return_value=MagicMock()))
        original_release = _ExecutionGuard._release

        def release_then_fire(guard):
            guard._timeout_event.set()
            original_release(guard)

        monkeypatch.setattr(_ExecutionGuard, "_release", release_then_fire)

        with pytest.raises(_TransientAttemptFailure):
            submitter._attempt("raise Exception('Pool not running')", 1, time.monotonic())

    def test_attempt_releases_session_for_unclassified_error(self, submitter, monkeypatch):
        submitter._pool.acquire.return_value = "sid-1"
        monkeypatch.setattr(submitter, "_get_or_create_spark", Mock(return_value=MagicMock()))

        with pytest.raises(DbtRuntimeError):
            submitter._attempt("raise ValueError('boom')", 1, time.monotonic())

        submitter._pool.release.assert_called_once_with("sid-1")
        submitter._pool.terminate.assert_not_called()

    @pytest.mark.parametrize(
        "session_ended, retryable, expected",
        [
            (True, True, "Athena terminated Spark session sid-1"),
            (True, False, "Athena terminated Spark session sid-1"),
            (False, False, "category 'capacity' is not in spark_connect_retry_on"),
            (False, True, r"failed after 3 attempts \(last session sid-1\)"),
        ],
    )
    def test_final_error_message_follows_failure_attributes(
        self, submitter, session_ended, retryable, expected
    ):
        failure = _TransientAttemptFailure(
            Exception("oops"), "sid-1", session_ended, retryable, "capacity"
        )

        error = submitter._final_error(failure, 3)

        assert re.search(expected, str(error))
        assert type(error).__name__ == (
            "SparkSessionTerminatedError" if session_ended else "DbtRuntimeError"
        )
        assert "oops" in str(error)


class TestExecutionGuard:
    @pytest.fixture
    def keepalive_cls(self, monkeypatch):
        cls = Mock()
        monkeypatch.setattr("dbt.adapters.athena.spark_connect.job.SessionKeepalive", cls)
        return cls

    def _guard(self, spark, *, budget=60.0, keepalive_interval=300, event=None):
        return _ExecutionGuard(
            spark=spark,
            session_id="sid-1",
            relation_name="rel",
            timeout=budget,
            budget=budget,
            keepalive_interval=keepalive_interval,
            timeout_event=event or threading.Event(),
        )

    def test_tags_during_body_and_cleans_up_after(self, keepalive_cls):
        spark = MagicMock()
        guard = self._guard(spark)

        with guard:
            tag = spark.addTag.call_args.args[0]
            keepalive_cls.return_value.start.assert_called_once()
            spark.removeTag.assert_not_called()

        keepalive_cls.return_value.stop.assert_called_once()
        spark.removeTag.assert_called_once_with(tag)
        assert guard._timer is not None and not guard._timer.is_alive()

    def test_cleans_up_when_body_raises(self, keepalive_cls):
        spark = MagicMock()
        guard = self._guard(spark)

        with pytest.raises(RuntimeError, match="body failed"):
            with guard:
                raise RuntimeError("body failed")

        keepalive_cls.return_value.stop.assert_called_once()
        spark.removeTag.assert_called_once()
        assert not guard._timer.is_alive()

    def test_zero_interval_starts_no_keepalive(self, keepalive_cls):
        with self._guard(MagicMock(), keepalive_interval=0):
            pass

        keepalive_cls.assert_not_called()

    def test_failed_start_still_cleans_up(self, keepalive_cls):
        spark = MagicMock()
        keepalive_cls.return_value.start.side_effect = RuntimeError("cannot start")
        guard = self._guard(spark)

        with pytest.raises(RuntimeError, match="cannot start"):
            guard.__enter__()

        keepalive_cls.return_value.stop.assert_called_once()
        spark.removeTag.assert_called_once()

    def test_failed_timer_start_propagates_original_error_and_removes_tag(
        self, keepalive_cls, monkeypatch
    ):
        spark = MagicMock()

        def failing_start(_timer):
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(threading.Timer, "start", failing_start)

        with pytest.raises(RuntimeError, match="can't start new thread"):
            self._guard(spark).__enter__()

        keepalive_cls.return_value.stop.assert_called_once()
        spark.removeTag.assert_called_once()

    def test_failed_keepalive_thread_start_propagates_original_error_and_removes_tag(
        self, monkeypatch
    ):
        spark = MagicMock()

        def failing_start(_thread):
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(threading.Thread, "start", failing_start)

        with pytest.raises(RuntimeError, match="can't start new thread"):
            self._guard(spark).__enter__()

        spark.removeTag.assert_called_once()

    def test_failed_add_tag_removes_no_tag(self, keepalive_cls):
        spark = MagicMock()
        spark.addTag.side_effect = RuntimeError("no tag")

        with pytest.raises(RuntimeError, match="no tag"):
            self._guard(spark).__enter__()

        spark.removeTag.assert_not_called()

    def test_remove_tag_failure_is_ignored(self, keepalive_cls):
        spark = MagicMock()
        spark.removeTag.side_effect = RuntimeError("gone")

        with self._guard(spark):
            pass

    def test_watchdog_interrupts_tag_and_sets_event(self, keepalive_cls):
        spark = MagicMock()
        event = threading.Event()

        with self._guard(spark, budget=0.01, event=event):
            assert event.wait(5)

        spark.interruptTag.assert_called_once_with(spark.addTag.call_args.args[0])


class TestDpuRequestComputation:
    """The submitter must reserve the tightest correct upper bound against
    the DPU budget: ``min(MaxConcurrentDpus, maxExecutors + 1)``.

    Independent of the pool — covers only ``_dpu_request`` derivation."""

    def test_spark_max_executors_helper_reads_from_classifications(self):
        ec = {
            "MaxConcurrentDpus": 4,
            "Classifications": [
                {
                    "Name": "spark-defaults",
                    "Properties": {"spark.dynamicAllocation.maxExecutors": "16"},
                }
            ],
        }
        assert _spark_max_executors(ec) == 16

    def test_spark_max_executors_helper_returns_none_when_absent(self):
        assert _spark_max_executors({"MaxConcurrentDpus": 4}) is None
        assert (
            _spark_max_executors({"MaxConcurrentDpus": 4, "Classifications": [{"Name": "other"}]})
            is None
        )

    def test_spark_max_executors_helper_returns_none_for_non_integer_value(self):
        ec = {
            "MaxConcurrentDpus": 4,
            "Classifications": [
                {
                    "Name": "spark-defaults",
                    "Properties": {"spark.dynamicAllocation.maxExecutors": "not-a-number"},
                }
            ],
        }
        assert _spark_max_executors(ec) is None

    def _make_submitter_with_engine_config(self, engine_config, mock_credentials):
        submitter = SparkConnectSubmitter(
            athena_client=Mock(),
            credentials=mock_credentials,
            config=Mock(spark_engine_version="3.5"),
            engine_config=engine_config,
            timeout=5,
            polling_interval=0.01,
            relation_name="rel",
        )
        return submitter

    @pytest.fixture
    def mock_credentials(self):
        return AthenaCredentials(
            database="db", schema="schema", region_name="us-east-1", spark_work_group="wg"
        )

    def test_acquire_session_leaves_connect_mode_env_alone(self, mock_credentials, monkeypatch):
        monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)
        submitter = self._make_submitter_with_engine_config(
            {"MaxConcurrentDpus": 4}, mock_credentials
        )
        submitter._pool = Mock()

        submitter._acquire_session(1.0)

        assert "SPARK_CONNECT_MODE_ENABLED" not in os.environ

    def test_credentials_values_reach_pool_acquire(self, mock_credentials):
        mock_credentials.spark_connect_max_sessions = 5
        mock_credentials.spark_connect_session_concurrency = 3
        mock_credentials.spark_connect_dpu_budget = 40
        submitter = self._make_submitter_with_engine_config(
            {"MaxConcurrentDpus": 4}, mock_credentials
        )
        submitter._pool = Mock()
        submitter._pool.acquire.return_value = "sid"

        assert submitter._acquire_session(12.5) == "sid"

        kwargs = submitter._pool.acquire.call_args.kwargs
        assert kwargs["max_sessions"] == 5
        assert kwargs["session_concurrency"] == 3
        assert kwargs["dpu_budget"] == 40
        assert kwargs["dpu_request"] == 4
        assert kwargs["timeout"] == 12.5

    def test_empty_code_does_not_evaluate_engine_config(self, mock_credentials):
        submitter = self._make_submitter_with_engine_config({}, mock_credentials)

        assert submitter.submit("  \n") == {"SparkConnect": True, "SparkSessionId": None}

    def test_derived_values_follow_current_inputs(self, mock_credentials):
        submitter = self._make_submitter_with_engine_config(
            {"MaxConcurrentDpus": 4}, mock_credentials
        )
        before = submitter._dpu_request
        submitter.engine_config = {"MaxConcurrentDpus": 2}

        assert (before, submitter._dpu_request) == (4, 2)

    def test_dpu_request_uses_min_of_dpus_and_executors_plus_driver(self, mock_credentials):
        ec = {
            "MaxConcurrentDpus": 4,
            "Classifications": [
                {
                    "Name": "spark-defaults",
                    "Properties": {"spark.dynamicAllocation.maxExecutors": "16"},
                }
            ],
        }
        submitter = self._make_submitter_with_engine_config(ec, mock_credentials)
        assert submitter._dpu_request == 4  # min(4, 17)

    def test_dpu_request_uses_executors_plus_driver_when_smaller_than_dpus(self, mock_credentials):
        ec = {
            "MaxConcurrentDpus": 20,
            "Classifications": [
                {
                    "Name": "spark-defaults",
                    "Properties": {"spark.dynamicAllocation.maxExecutors": "3"},
                }
            ],
        }
        submitter = self._make_submitter_with_engine_config(ec, mock_credentials)
        assert submitter._dpu_request == 4  # min(20, 4)

    def test_dpu_request_falls_back_to_max_concurrent_when_executors_absent(
        self, mock_credentials
    ):
        ec = {"MaxConcurrentDpus": 4}
        submitter = self._make_submitter_with_engine_config(ec, mock_credentials)
        assert submitter._dpu_request == 4


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

    def test_session_ended_client_error_is_raised_without_polling(self):
        stopped = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "InvalidRequestException",
                    "Message": "Can not generate Session endpoint URL for Session in STOPPED state",
                }
            },
            operation_name="GetSessionEndpoint",
        )
        client = Mock()
        client.get_session_endpoint.side_effect = [
            stopped,
            {"EndpointUrl": "https://x", "AuthToken": "tok"},
        ]
        submitter = self._make_submitter(client)

        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            submitter._wait_for_endpoint("sid", remaining_budget=10)

        assert excinfo.value is stopped
        assert client.get_session_endpoint.call_count == 1
