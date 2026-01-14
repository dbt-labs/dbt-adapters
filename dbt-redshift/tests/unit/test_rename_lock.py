import threading
import time
from multiprocessing import get_context
from unittest import TestCase, mock

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from dbt.adapters.redshift.connections import RedshiftConnectionManager
from tests.unit.utils import (
    config_from_parts_or_dicts,
    inject_adapter,
)


class TestRenameLockMechanism(TestCase):
    """Tests for the schema-level rename locking mechanism itself."""

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

        # Reset the class-level schema locks before each test
        RedshiftConnectionManager._schema_locks = {}
        RedshiftConnectionManager._schema_locks_lock = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = RedshiftAdapter(self.config, get_context("spawn"))
            inject_adapter(self._adapter, RedshiftPlugin)
        return self._adapter

    def test_get_schema_lock_creates_lock(self):
        """Test that _get_schema_lock creates a new lock for a schema."""
        lock1 = RedshiftConnectionManager._get_schema_lock("test_schema")
        self.assertIsNotNone(lock1)
        self.assertIn("test_schema", RedshiftConnectionManager._schema_locks)

    def test_get_schema_lock_returns_same_lock(self):
        """Test that _get_schema_lock returns the same lock for the same schema."""
        lock1 = RedshiftConnectionManager._get_schema_lock("test_schema")
        lock2 = RedshiftConnectionManager._get_schema_lock("test_schema")
        self.assertIs(lock1, lock2)

    def test_get_schema_lock_different_schemas(self):
        """Test that different schemas get different locks."""
        lock1 = RedshiftConnectionManager._get_schema_lock("schema_a")
        lock2 = RedshiftConnectionManager._get_schema_lock("schema_b")
        self.assertIsNot(lock1, lock2)

    def test_get_schema_lock_case_insensitive(self):
        """Test that schema names are normalized to lowercase."""
        # Use unique names for this test to avoid conflicts with parallel test execution
        lock1 = RedshiftConnectionManager._get_schema_lock("CASE_TEST_SCHEMA")
        lock2 = RedshiftConnectionManager._get_schema_lock("case_test_schema")
        self.assertIs(lock1, lock2)
        # Verify they're stored under the lowercase key
        self.assertIn("case_test_schema", RedshiftConnectionManager._schema_locks)
        self.assertNotIn("CASE_TEST_SCHEMA", RedshiftConnectionManager._schema_locks)

    def test_rename_lock_context_manager(self):
        """Test that rename_lock can be used as a context manager."""
        with self.adapter.connections.rename_lock("test_schema"):
            pass  # Should not raise

    def test_rename_lock_serializes_same_schema(self):
        """Test that rename_lock serializes operations on the same schema."""
        execution_order = []
        barrier = threading.Barrier(2)

        def operation(name, schema):
            barrier.wait()  # Ensure both threads start at the same time
            with self.adapter.connections.rename_lock(schema):
                execution_order.append(f"{name}_start")
                time.sleep(0.1)  # Simulate some work
                execution_order.append(f"{name}_end")

        thread1 = threading.Thread(target=operation, args=("op1", "same_schema"))
        thread2 = threading.Thread(target=operation, args=("op2", "same_schema"))

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        # With locking, operations should be serialized
        # Either op1_start, op1_end, op2_start, op2_end
        # or op2_start, op2_end, op1_start, op1_end
        self.assertEqual(len(execution_order), 4)

        # Check that operations don't interleave
        if execution_order[0] == "op1_start":
            self.assertEqual(execution_order, ["op1_start", "op1_end", "op2_start", "op2_end"])
        else:
            self.assertEqual(execution_order, ["op2_start", "op2_end", "op1_start", "op1_end"])

    def test_rename_lock_allows_parallel_different_schemas(self):
        """Test that rename_lock allows parallel operations on different schemas."""
        execution_log = []
        barrier = threading.Barrier(2)
        lock = threading.Lock()

        def operation(name, schema):
            barrier.wait()  # Ensure both threads start at the same time
            with self.adapter.connections.rename_lock(schema):
                with lock:
                    execution_log.append(f"{name}_start")
                time.sleep(0.1)  # Simulate some work
                with lock:
                    execution_log.append(f"{name}_end")

        thread1 = threading.Thread(target=operation, args=("op1", "schema_a"))
        thread2 = threading.Thread(target=operation, args=("op2", "schema_b"))

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        # With different schemas, operations can run in parallel
        # So we might see interleaved execution like:
        # op1_start, op2_start, op1_end, op2_end
        self.assertEqual(len(execution_log), 4)

        # Both starts should happen before both ends (due to parallel execution)
        # But we can't guarantee exact interleaving due to timing
        # Just verify all operations completed
        self.assertEqual(set(execution_log), {"op1_start", "op1_end", "op2_start", "op2_end"})


class TestRenameLockBehaviorFlag(TestCase):
    """Tests for the enable_rename_relation_lock behavior flag."""

    def setUp(self):
        # Reset the class-level schema locks before each test
        RedshiftConnectionManager._schema_locks = {}
        RedshiftConnectionManager._schema_locks_lock = None

    def _create_adapter(self, flags=None):
        """Create an adapter with the given flags configuration."""
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

        if flags:
            project_cfg["flags"] = flags

        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        adapter = RedshiftAdapter(config, get_context("spawn"))
        inject_adapter(adapter, RedshiftPlugin)
        return adapter

    def test_behavior_flag_defaults_to_false(self):
        """Test that enable_rename_relation_lock defaults to False."""
        adapter = self._create_adapter()
        self.assertFalse(adapter.behavior.enable_rename_relation_lock)

    def test_behavior_flag_can_be_enabled(self):
        """Test that enable_rename_relation_lock can be enabled via flags."""
        adapter = self._create_adapter(flags={"enable_rename_relation_lock": True})
        self.assertTrue(adapter.behavior.enable_rename_relation_lock)

    @mock.patch("redshift_connector.connect", mock.MagicMock())
    def test_rename_relation_uses_lock_when_flag_enabled(self):
        """Test that rename_relation uses the schema lock when flag is enabled."""
        adapter = self._create_adapter(flags={"enable_rename_relation_lock": True})

        # Create mock relations
        from_relation = mock.MagicMock()
        from_relation.schema = "test_schema"
        to_relation = mock.MagicMock()

        # Mock the parent class's rename_relation
        with mock.patch.object(
            RedshiftAdapter.__bases__[0], "rename_relation"
        ) as mock_parent_rename:
            # Mock the rename_lock context manager to track calls
            original_rename_lock = adapter.connections.rename_lock

            calls = []

            def tracking_rename_lock(schema):
                calls.append(schema)
                return original_rename_lock(schema)

            adapter.connections.rename_lock = tracking_rename_lock

            # Call rename_relation
            adapter.rename_relation(from_relation, to_relation)

            # Verify the lock was acquired with the correct schema
            self.assertEqual(calls, ["test_schema"])

            # Verify the parent method was called
            mock_parent_rename.assert_called_once_with(from_relation, to_relation)

    @mock.patch("redshift_connector.connect", mock.MagicMock())
    def test_rename_relation_skips_lock_when_flag_disabled(self):
        """Test that rename_relation does not use the lock when flag is disabled."""
        adapter = self._create_adapter(flags={"enable_rename_relation_lock": False})

        # Create mock relations
        from_relation = mock.MagicMock()
        from_relation.schema = "test_schema"
        to_relation = mock.MagicMock()

        # Mock the parent class's rename_relation
        with mock.patch.object(
            RedshiftAdapter.__bases__[0], "rename_relation"
        ) as mock_parent_rename:
            # Mock the rename_lock context manager to track calls
            original_rename_lock = adapter.connections.rename_lock

            calls = []

            def tracking_rename_lock(schema):
                calls.append(schema)
                return original_rename_lock(schema)

            adapter.connections.rename_lock = tracking_rename_lock

            # Call rename_relation
            adapter.rename_relation(from_relation, to_relation)

            # Verify the lock was NOT acquired
            self.assertEqual(calls, [])

            # Verify the parent method was still called
            mock_parent_rename.assert_called_once_with(from_relation, to_relation)

    @mock.patch("redshift_connector.connect", mock.MagicMock())
    def test_rename_relation_handles_none_schema_when_flag_enabled(self):
        """Test that rename_relation handles None schema gracefully when flag is enabled."""
        adapter = self._create_adapter(flags={"enable_rename_relation_lock": True})

        from_relation = mock.MagicMock()
        from_relation.schema = None
        to_relation = mock.MagicMock()

        with mock.patch.object(
            RedshiftAdapter.__bases__[0], "rename_relation"
        ) as mock_parent_rename:
            # Should not raise even with None schema
            adapter.rename_relation(from_relation, to_relation)
            mock_parent_rename.assert_called_once()
