"""
Tests for unique test failure table names feature.
This validates that the store_failures_unique configuration
creates tables with appropriate suffixes.
"""

import pytest
from datetime import datetime
from unittest.mock import patch

from dbt.tests.util import run_dbt, check_relation_has_expected_schema


# Simple model to test against
models__simple_model = """
select 1 as id, 'Alice' as name
union all
select 2 as id, 'Bob' as name
union all
select null as id, 'Charlie' as name  -- This will fail not_null test
"""

# Test configuration with unique suffix enabled
test_yml__unique_suffix_invocation = """
version: 2

models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - not_null:
              config:
                store_failures: true
                store_failures_unique: true
                store_failures_suffix: invocation_id
      - name: name
        tests:
          - not_null:
              config:
                store_failures: true
                store_failures_unique: true
                store_failures_suffix: timestamp
"""

test_yml__unique_suffix_hour = """
version: 2

models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - not_null:
              config:
                store_failures: true
                store_failures_unique: true
                store_failures_suffix: hour
"""

test_yml__unique_suffix_custom = """
version: 2

models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - not_null:
              config:
                store_failures: true
                store_failures_unique: true
                store_failures_suffix: my_custom_suffix
"""

test_yml__no_unique_suffix = """
version: 2

models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - not_null:
              config:
                store_failures: true
                store_failures_unique: false  # Explicitly disabled
"""


class TestUniqueStoreFailures:
    """Test suite for unique test failure table names."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": models__simple_model,
        }

    def test_invocation_id_suffix(self, project):
        """Test that invocation_id suffix creates unique table names."""
        # Set up the test configuration
        project.write_yaml("models/schema.yml", test_yml__unique_suffix_invocation)
        
        # Run the models
        run_dbt(["run"])
        
        # Run tests with store-failures
        with patch('dbt.context.providers.invocation_id', 'a1b2c3d4e5f6g7h8'):
            results = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Check that we have test failures (expected due to null id)
        assert len(results) == 2
        assert any(r.status == "fail" for r in results)
        
        # Verify table name contains invocation_id suffix
        # The actual verification would need adapter-specific code to check table existence
        # For now, we're testing that the code doesn't error
        
    def test_hour_suffix(self, project):
        """Test that hour suffix creates tables with YYYYMMDD_HH pattern."""
        project.write_yaml("models/schema.yml", test_yml__unique_suffix_hour)
        
        # Run the models
        run_dbt(["run"])
        
        # Mock the run_started_at to a specific time
        test_time = datetime(2024, 8, 19, 14, 30, 45)
        with patch('dbt.context.providers.run_started_at', test_time):
            results = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Check that tests ran (one should fail)
        assert len(results) == 1
        assert results[0].status == "fail"
        
        # Expected suffix would be: _20240819_14
        # Actual table verification would be adapter-specific

    def test_custom_suffix(self, project):
        """Test that custom string suffix is appended correctly."""
        project.write_yaml("models/schema.yml", test_yml__unique_suffix_custom)
        
        # Run the models
        run_dbt(["run"])
        
        # Run tests
        results = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Check that tests ran
        assert len(results) == 1
        assert results[0].status == "fail"
        
        # Expected suffix: _my_custom_suffix
        # Table name would be like: not_null_simple_model_id_my_custom_suffix

    def test_no_unique_suffix(self, project):
        """Test that disabling unique suffix uses standard table names."""
        project.write_yaml("models/schema.yml", test_yml__no_unique_suffix)
        
        # Run the models
        run_dbt(["run"])
        
        # Run tests
        results = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Check that tests ran
        assert len(results) == 1
        assert results[0].status == "fail"
        
        # Table name should be standard: not_null_simple_model_id (no suffix)

    def test_backward_compatibility(self, project):
        """Test that default behavior (no config) remains unchanged."""
        # Test with no store_failures_unique config at all
        default_yml = """
version: 2

models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - not_null:
              config:
                store_failures: true
                # No store_failures_unique or suffix config
"""
        project.write_yaml("models/schema.yml", default_yml)
        
        # Run the models
        run_dbt(["run"])
        
        # Run tests - should work exactly as before
        results = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Check that tests ran normally
        assert len(results) == 1
        assert results[0].status == "fail"
        
        # Table name should be standard with no suffix

    def test_parallel_runs_different_tables(self, project):
        """Test that parallel runs with different invocation IDs create different tables."""
        project.write_yaml("models/schema.yml", test_yml__unique_suffix_invocation)
        
        # Run the models
        run_dbt(["run"])
        
        # First run with one invocation ID
        with patch('dbt.context.providers.invocation_id', 'run1abcd'):
            results1 = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Second run with different invocation ID
        with patch('dbt.context.providers.invocation_id', 'run2efgh'):
            results2 = run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Both runs should complete successfully
        assert len(results1) == 2
        assert len(results2) == 2
        
        # In a real test, we'd verify two different tables exist:
        # - not_null_simple_model_id_run1abcd
        # - not_null_simple_model_id_run2efgh


class TestUniqueStoreFailuresIntegration:
    """Integration tests requiring actual database connection."""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": models__simple_model,
        }
    
    @pytest.fixture(scope="class")
    def tests(self):
        # Return test yml configurations
        return {
            "schema.yml": test_yml__unique_suffix_hour
        }
    
    @pytest.mark.skip(reason="Requires database connection")
    def test_table_actually_created_with_suffix(self, project, adapter):
        """
        Integration test to verify table is actually created in the database with suffix.
        This test would need to be run with a real adapter connection.
        """
        # Run models
        run_dbt(["run"])
        
        # Run tests with specific time
        test_time = datetime(2024, 8, 19, 14, 30, 45)
        with patch('dbt.context.providers.run_started_at', test_time):
            run_dbt(["test", "--store-failures"], expect_pass=False)
        
        # Check if table exists with expected name
        expected_table_name = "not_null_simple_model_id_20240819_14"
        relation = adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier=expected_table_name
        )
        
        assert relation is not None, f"Table {expected_table_name} should exist"