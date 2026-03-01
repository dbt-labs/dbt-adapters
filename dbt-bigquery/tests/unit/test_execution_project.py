"""Unit tests for per-node execution_project feature."""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
from threading import get_ident

import google.cloud.bigquery
from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.credentials import BigQueryConnectionMethod, Priority
from dbt.adapters.bigquery.impl import BigQueryAdapter


class TestExecutionProjectConnectionManager(unittest.TestCase):
    """Test the connection manager's execution_project switching functionality."""

    def setUp(self):
        self.credentials = Mock(BigQueryCredentials)
        self.credentials.method = BigQueryConnectionMethod.OAUTH
        self.credentials.database = "test-project"
        self.credentials.schema = "test_schema"
        self.credentials.execution_project = "test-project"
        self.credentials.job_retries = 1
        self.credentials.job_retry_deadline_seconds = 1
        self.credentials.scopes = tuple()
        self.credentials.job_execution_timeout_seconds = 1
        self.credentials.priority = Priority.Interactive
        self.credentials.location = "US"
        self.credentials.maximum_bytes_billed = None

        # Mock the to_dict method to simulate the real behavior
        self.credentials.to_dict.return_value = {
            "database": "test-project",
            "schema": "test_schema",
            "method": "oauth",
            "execution_project": "test-project",
            "priority": "interactive",
            "scopes": ["scope1", "scope2"],  # List to test conversion
            "location": "US",
            "job_retries": 1,
            # Include some aliases that should be removed
            "project": "test-project",
            "dataset": "test_schema",
        }

        self.mock_client = Mock(google.cloud.bigquery.Client)
        self.mock_client.close = Mock()

        self.mock_connection = MagicMock()
        self.mock_connection.handle = self.mock_client
        self.mock_connection.credentials = self.credentials

        self.mp_context = Mock()
        self.mp_context.RLock.return_value = (
            MagicMock()
        )  # MagicMock supports context manager protocol

        self.connections = BigQueryConnectionManager(
            profile=Mock(credentials=self.credentials, query_comment=None),
            mp_context=self.mp_context,
        )
        # Add cache_lock that supports context manager
        self.connections.cache_lock = MagicMock()
        self.connections.client_cache = {}
        self.connections.thread_execution_projects = {}
        self.connections.get_thread_connection = lambda: self.mock_connection
        self.connections.get_thread_identifier = lambda: get_ident()

    @patch("dbt.adapters.bigquery.connections.create_bigquery_client")
    def test_get_client_for_project_creates_new_client(self, mock_create_client):
        """Test that get_client_for_project creates and caches a new client."""
        new_client = Mock()
        mock_create_client.return_value = new_client

        # First call should create a new client
        result = self.connections.get_client_for_project("compute-project")

        # Should have created a new client
        mock_create_client.assert_called_once()
        assert result == new_client

        # Client should be marked as cached
        assert hasattr(new_client, "_dbt_cached")
        assert new_client._dbt_cached is True

        # Client should be in cache
        assert "compute-project" in self.connections.client_cache
        assert self.connections.client_cache["compute-project"] == new_client

    @patch("dbt.adapters.bigquery.connections.create_bigquery_client")
    def test_get_client_for_project_reuses_cached_client(self, mock_create_client):
        """Test that get_client_for_project reuses cached clients."""
        cached_client = Mock()
        self.connections.client_cache["compute-project"] = cached_client

        # Should reuse cached client
        result = self.connections.get_client_for_project("compute-project")

        # Should not create a new client
        mock_create_client.assert_not_called()
        assert result == cached_client

    def test_create_modified_credentials_handles_aliases(self):
        """Test that _create_modified_credentials properly handles credential aliases."""
        modified_creds = self.connections._create_modified_credentials("compute-project")

        # Should be a BigQueryCredentials instance
        assert isinstance(modified_creds, BigQueryCredentials)

        # Execution project should be updated
        assert modified_creds.execution_project == "compute-project"

        # Database and schema should be preserved
        assert modified_creds.database == "test-project"
        assert modified_creds.schema == "test_schema"

        # Scopes should be converted back to tuple
        assert isinstance(modified_creds.scopes, tuple)

        # Method and priority should be enums
        assert modified_creds.method == BigQueryConnectionMethod.OAUTH
        assert modified_creds.priority == Priority.Interactive

    @patch("dbt.adapters.bigquery.connections.create_bigquery_client")
    def test_switch_execution_project(self, mock_create_client):
        """Test switching execution project for a connection."""
        new_client = Mock()
        mock_create_client.return_value = new_client

        # Switch to a different project
        previous = self.connections.switch_execution_project("compute-project")

        # Should return the previous project
        assert previous == "test-project"

        # Connection should now use the new client
        assert self.mock_connection.handle == new_client

        # Thread should be tracked with new project
        thread_id = self.connections.get_thread_identifier()
        assert self.connections.thread_execution_projects[thread_id] == "compute-project"

    def test_switch_execution_project_same_project(self):
        """Test that switching to the same project is a no-op."""
        original_handle = self.mock_connection.handle

        # Switch to the same project
        previous = self.connections.switch_execution_project("test-project")

        # Should return the same project
        assert previous == "test-project"

        # Connection handle should not change
        assert self.mock_connection.handle == original_handle

    def test_switch_execution_project_none(self):
        """Test that switching to None is a no-op."""
        original_handle = self.mock_connection.handle

        # Switch to None (use default)
        previous = self.connections.switch_execution_project(None)

        # Should return the current project
        assert previous == "test-project"

        # Connection handle should not change
        assert self.mock_connection.handle == original_handle

    def test_cleanup_all_cleans_cached_clients(self):
        """Test that cleanup_all properly cleans up cached clients."""
        # Add some cached clients
        client1 = Mock()
        client2 = Mock()
        self.connections.client_cache = {
            "project1": client1,
            "project2": client2,
        }
        self.connections.thread_execution_projects = {
            "thread1": "project1",
            "thread2": "project2",
        }

        # Mock parent cleanup_all and manually call our cleanup logic
        with patch.object(BigQueryConnectionManager.__bases__[0], "cleanup_all"):
            # Call the actual cleanup_all method
            BigQueryConnectionManager.cleanup_all(self.connections)

        # All cached clients should be closed
        client1.close.assert_called_once()
        client2.close.assert_called_once()

        # Caches should be cleared
        assert len(self.connections.client_cache) == 0
        assert len(self.connections.thread_execution_projects) == 0

    def test_close_does_not_close_cached_clients(self):
        """Test that close() doesn't close cached clients."""
        # Mark client as cached
        self.mock_connection.handle._dbt_cached = True

        # Close the connection
        self.connections.close(self.mock_connection)

        # Should not close the cached client
        self.mock_connection.handle.close.assert_not_called()

    def test_close_closes_non_cached_clients(self):
        """Test that close() closes non-cached clients."""
        # Client is not marked as cached
        if hasattr(self.mock_connection.handle, "_dbt_cached"):
            del self.mock_connection.handle._dbt_cached

        # Close the connection
        self.connections.close(self.mock_connection)

        # Should close the non-cached client
        self.mock_connection.handle.close.assert_called_once()


class TestExecutionProjectAdapter(unittest.TestCase):
    """Test the adapter's execution_project hooks."""

    def setUp(self):
        self.adapter = Mock(spec=BigQueryAdapter)
        self.adapter.connections = Mock()
        self.adapter.config = Mock()
        self.adapter.config.credentials = Mock()
        self.adapter.config.credentials.execution_project = "default-project"

        # Use the real methods
        self.adapter.pre_model_hook = BigQueryAdapter.pre_model_hook.__get__(
            self.adapter, BigQueryAdapter
        )
        self.adapter.post_model_hook = BigQueryAdapter.post_model_hook.__get__(
            self.adapter, BigQueryAdapter
        )

    def test_pre_model_hook_with_execution_project(self):
        """Test pre_model_hook switches to model's execution_project."""
        config = {"execution_project": "model-compute-project"}

        self.adapter.connections.switch_execution_project.return_value = "default-project"

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should switch to model's execution_project
        self.adapter.connections.switch_execution_project.assert_called_once_with(
            "model-compute-project"
        )

        # Should return the previous project as context
        assert context == "default-project"

    def test_pre_model_hook_without_execution_project(self):
        """Test pre_model_hook does nothing without execution_project."""
        config = {"some_other_config": "value"}

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should not switch execution_project
        self.adapter.connections.switch_execution_project.assert_not_called()

        # Should return None as context
        assert context is None

    def test_pre_model_hook_with_default_execution_project(self):
        """Test pre_model_hook with execution_project same as default."""
        config = {"execution_project": "default-project"}

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should not switch since it's the same as default
        self.adapter.connections.switch_execution_project.assert_not_called()

        # Should return None as context
        assert context is None

    def test_post_model_hook_restores_previous_project(self):
        """Test post_model_hook restores the previous execution_project."""
        config = {"execution_project": "model-compute-project"}
        context = "previous-project"  # Context from pre_model_hook

        # Call post_model_hook
        self.adapter.post_model_hook(config, context)

        # Should restore the previous project
        self.adapter.connections.switch_execution_project.assert_called_once_with(
            "previous-project"
        )

    def test_post_model_hook_without_context(self):
        """Test post_model_hook does nothing without context."""
        config = {"execution_project": "model-compute-project"}
        context = None

        # Call post_model_hook
        self.adapter.post_model_hook(config, context)

        # Should not switch since there's no context
        self.adapter.connections.switch_execution_project.assert_not_called()

    def test_hook_integration(self):
        """Test that pre and post hooks work together correctly."""
        config = {"execution_project": "compute-project"}

        self.adapter.connections.switch_execution_project.return_value = "original-project"

        # Pre-hook switches to model's project
        context = self.adapter.pre_model_hook(config)
        assert context == "original-project"

        # Reset mock to test post-hook
        self.adapter.connections.switch_execution_project.reset_mock()

        # Post-hook restores original project
        self.adapter.post_model_hook(config, context)

        # Should restore the original project
        self.adapter.connections.switch_execution_project.assert_called_once_with(
            "original-project"
        )

    def test_pre_model_hook_with_empty_string_execution_project(self):
        """Test pre_model_hook ignores empty string execution_project."""
        config = {"execution_project": ""}

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should not switch execution_project for empty string
        self.adapter.connections.switch_execution_project.assert_not_called()

        # Should return None as context
        assert context is None

    def test_pre_model_hook_with_whitespace_execution_project(self):
        """Test pre_model_hook ignores whitespace-only execution_project."""
        config = {"execution_project": "   "}

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should not switch execution_project for whitespace-only string
        self.adapter.connections.switch_execution_project.assert_not_called()

        # Should return None as context
        assert context is None

    def test_pre_model_hook_with_dict_mapping_matching_target(self):
        """Test pre_model_hook with dict mapping that matches current target."""
        # Set the adapter's target name
        self.adapter.config.target_name = "prod"

        config = {
            "execution_project": {
                "dev": "dev-sandbox",
                "prod": "production-project",
                "ci": "ci-sandbox",
            }
        }

        # Mock switch_execution_project to return previous project
        self.adapter.connections.switch_execution_project.return_value = "original-project"

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should switch to the mapped execution_project
        self.adapter.connections.switch_execution_project.assert_called_once_with(
            "production-project"
        )

        # Should return the previous project as context
        assert context == "original-project"

    def test_pre_model_hook_with_dict_mapping_no_match(self):
        """Test pre_model_hook with dict mapping that doesn't match current target."""
        # Set the adapter's target name
        self.adapter.config.target_name = "staging"

        config = {
            "execution_project": {
                "dev": "dev-sandbox",
                "prod": "production-project",
                "ci": "ci-sandbox",
            }
        }

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should not switch execution_project when no mapping matches
        self.adapter.connections.switch_execution_project.assert_not_called()

        # Should return None as context
        assert context is None

    def test_pre_model_hook_with_dict_mapping_empty_value(self):
        """Test pre_model_hook with dict mapping that maps to empty string."""
        # Set the adapter's target name
        self.adapter.config.target_name = "dev"

        config = {"execution_project": {"dev": "", "prod": "production-project"}}  # Empty string

        # Call pre_model_hook
        context = self.adapter.pre_model_hook(config)

        # Should not switch execution_project for empty string value
        self.adapter.connections.switch_execution_project.assert_not_called()

        # Should return None as context
        assert context is None


class TestExecutionProjectValidation(unittest.TestCase):
    """Test validation of execution_project values."""

    def setUp(self):
        self.credentials = Mock(BigQueryCredentials)
        self.credentials.method = BigQueryConnectionMethod.OAUTH
        self.credentials.database = "test-project"
        self.credentials.schema = "test_schema"
        self.credentials.execution_project = "test-project"
        self.credentials.job_retries = 1
        self.credentials.job_retry_deadline_seconds = 1
        self.credentials.scopes = tuple()
        self.credentials.job_execution_timeout_seconds = 1
        self.credentials.priority = Priority.Interactive
        self.credentials.location = "US"
        self.credentials.maximum_bytes_billed = None

        # Mock the to_dict method to simulate the real behavior
        self.credentials.to_dict.return_value = {
            "database": "test-project",
            "schema": "test_schema",
            "method": "oauth",
            "execution_project": "test-project",
            "priority": "interactive",
            "scopes": ["scope1", "scope2"],
            "location": "US",
            "job_retries": 1,
        }

        self.mock_client = Mock(google.cloud.bigquery.Client)
        self.mock_client.close = Mock()

        self.mock_connection = MagicMock()
        self.mock_connection.handle = self.mock_client
        self.mock_connection.credentials = self.credentials

        self.mp_context = Mock()
        self.mp_context.RLock.return_value = MagicMock()

        self.connections = BigQueryConnectionManager(
            profile=Mock(credentials=self.credentials, query_comment=None),
            mp_context=self.mp_context,
        )
        self.connections.cache_lock = MagicMock()
        self.connections.client_cache = {}
        self.connections.thread_execution_projects = {}
        self.connections.get_thread_connection = lambda: self.mock_connection
        self.connections.get_thread_identifier = lambda: get_ident()

    def test_switch_execution_project_invalid_characters(self):
        """Test that invalid project names are rejected."""
        from dbt_common.exceptions import DbtRuntimeError

        invalid_projects = [
            "UPPERCASE-PROJECT",  # uppercase not allowed
            "project_with_underscores",  # underscores not allowed
            "project@with@symbols",  # special characters not allowed
            "1starts-with-number",  # must start with letter
            "ends-with-hyphen-",  # cannot end with hyphen
            "a",  # too short (less than 6 chars)
            "a" * 31,  # too long (more than 30 chars)
        ]

        for invalid_project in invalid_projects:
            with self.assertRaises(DbtRuntimeError) as context:
                self.connections.switch_execution_project(invalid_project)

            assert "Invalid execution_project" in str(context.exception)

    def test_switch_execution_project_valid_names(self):
        """Test that valid project names are accepted."""
        # These are valid project names
        valid_projects = [
            "valid-project",
            "project-123",
            "my-data-warehouse-project",
            "abcdef",  # exactly 6 chars
            "a" * 28 + "bc",  # exactly 30 chars
        ]

        with patch(
            "dbt.adapters.bigquery.connections.create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.return_value = Mock()

            for valid_project in valid_projects:
                # Reset the thread execution projects so each project is considered new
                self.connections.thread_execution_projects = {}

                # Should not raise an error
                try:
                    self.connections.switch_execution_project(valid_project)
                except Exception as e:
                    self.fail(f"Valid project name '{valid_project}' raised an error: {e}")


if __name__ == "__main__":
    unittest.main()
