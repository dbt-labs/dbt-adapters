"""Tests for the Spark Connect submission path (Apache Spark 3.5+)."""

import sys
import time
from unittest.mock import MagicMock, Mock, patch

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
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
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
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
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
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
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
            "dbt.adapters.athena.spark_connect.job.create_athena_channel_builder",
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
