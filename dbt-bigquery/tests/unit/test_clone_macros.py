import unittest
from unittest import mock
from jinja2 import Environment, FileSystemLoader


class MacroReturn(Exception):
    """Exception raised to return a value from a Jinja macro"""

    def __init__(self, value):
        self.value = value


class TestCloneMacros(unittest.TestCase):
    """Unit tests for BigQuery clone materialization macros"""

    def setUp(self):
        """Set up Jinja environment and mock context for testing macros"""
        self.jinja_env = Environment(
            loader=FileSystemLoader("src/dbt/include/bigquery/macros"),
            extensions=["jinja2.ext.do"],
        )

        # Mock adapter with get_bq_table method
        self.mock_adapter = mock.Mock()

        # Default context that macros expect
        self.default_context = {
            "adapter": self.mock_adapter,
            "return": self.__return_func,  # Jinja return function
        }

    def __return_func(self, value):
        """Mock return function that raises an exception to simulate dbt's return behavior"""
        raise MacroReturn(value)

    def __get_template(self, template_filename):
        """Load a Jinja template with the default context"""
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, macro_name, *args):
        """Execute a macro from the template with given arguments"""
        try:
            module = template.make_module(self.default_context)
            getattr(module, macro_name)(*args)
            # If no exception was raised, return None
            return None
        except MacroReturn as e:
            return e.value

    def test_macros_load(self):
        """Verify that clone macros template loads without errors"""
        self.jinja_env.get_template("materializations/clone.sql")

    def test_is_clone_replaceable_no_target_relation(self):
        """When target relation doesn't exist, should return True (can replace)"""
        template = self.__get_template("materializations/clone.sql")

        # No target relation (None)
        result = self.__run_macro(template, "bigquery__is_clone_replaceable", None, mock.Mock())

        self.assertTrue(result)

    def test_is_clone_replaceable_no_target_table(self):
        """When target table doesn't exist in BigQuery, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock get_bq_table to return None for target (table doesn't exist)
        self.mock_adapter.get_bq_table.side_effect = [None, mock.Mock()]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_no_source_table(self):
        """When source table doesn't exist in BigQuery, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock get_bq_table to return table for target, None for source
        self.mock_adapter.get_bq_table.side_effect = [mock.Mock(), None]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_no_partitioning(self):
        """When neither table is partitioned, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock tables with no partitioning or clustering
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = None
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = None
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_matching_time_partitioning(self):
        """When both tables have matching time partitioning, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock tables with matching time partitioning
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = mock.Mock()
        target_table.time_partitioning.field = "created_at"
        target_table.partitioning_type = "DAY"
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = mock.Mock()
        source_table.time_partitioning.field = "created_at"
        source_table.partitioning_type = "DAY"
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_different_partition_field(self):
        """When partition fields differ, should return False (cannot replace)"""
        template = self.__get_template("materializations/clone.sql")

        # Target partitioned on created_at, source on updated_at
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = mock.Mock()
        target_table.time_partitioning.field = "created_at"
        target_table.partitioning_type = "DAY"
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = mock.Mock()
        source_table.time_partitioning.field = "updated_at"
        source_table.partitioning_type = "DAY"
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertFalse(result)

    def test_is_clone_replaceable_different_partition_granularity(self):
        """When partition granularity differs (DAY vs HOUR), should return False"""
        template = self.__get_template("materializations/clone.sql")

        # Target partitioned by DAY, source by HOUR
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = mock.Mock()
        target_table.time_partitioning.field = "created_at"
        target_table.partitioning_type = "DAY"
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = mock.Mock()
        source_table.time_partitioning.field = "created_at"
        source_table.partitioning_type = "HOUR"
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertFalse(result)

    def test_is_clone_replaceable_one_partitioned_one_not(self):
        """When only one table is partitioned, should return False"""
        template = self.__get_template("materializations/clone.sql")

        # Target is partitioned, source is not
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = mock.Mock()
        target_table.time_partitioning.field = "created_at"
        target_table.partitioning_type = "DAY"
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = None
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertFalse(result)

    def test_is_clone_replaceable_matching_range_partitioning(self):
        """When both tables have matching range partitioning, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock tables with matching range partitioning
        target_table = mock.Mock()
        target_table.time_partitioning = None
        target_table.range_partitioning = mock.Mock()
        target_table.range_partitioning.field = "id"
        target_table.range_partitioning.range_ = mock.Mock()
        target_table.range_partitioning.range_.start = 1
        target_table.range_partitioning.range_.end = 100
        target_table.range_partitioning.range_.interval = 10
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.time_partitioning = None
        source_table.range_partitioning = mock.Mock()
        source_table.range_partitioning.field = "id"
        source_table.range_partitioning.range_ = mock.Mock()
        source_table.range_partitioning.range_.start = 1
        source_table.range_partitioning.range_.end = 100
        source_table.range_partitioning.range_.interval = 10
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_different_range_interval(self):
        """When range partition intervals differ, should return False"""
        template = self.__get_template("materializations/clone.sql")

        # Target has interval of 10, source has interval of 5
        target_table = mock.Mock()
        target_table.time_partitioning = None
        target_table.range_partitioning = mock.Mock()
        target_table.range_partitioning.field = "id"
        target_table.range_partitioning.range_ = mock.Mock()
        target_table.range_partitioning.range_.start = 1
        target_table.range_partitioning.range_.end = 100
        target_table.range_partitioning.range_.interval = 10
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.time_partitioning = None
        source_table.range_partitioning = mock.Mock()
        source_table.range_partitioning.field = "id"
        source_table.range_partitioning.range_ = mock.Mock()
        source_table.range_partitioning.range_.start = 1
        source_table.range_partitioning.range_.end = 100
        source_table.range_partitioning.range_.interval = 5  # Different!
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertFalse(result)

    def test_is_clone_replaceable_matching_clustering(self):
        """When both tables have matching clustering fields, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock tables with matching clustering
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = None
        target_table.clustering_fields = ["col_a", "col_b"]

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = None
        source_table.clustering_fields = ["col_a", "col_b"]

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_different_clustering_fields(self):
        """When clustering fields differ, should return False"""
        template = self.__get_template("materializations/clone.sql")

        # Target clustered by col_a and col_b, source by col_a only
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = None
        target_table.clustering_fields = ["col_a", "col_b"]

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = None
        source_table.clustering_fields = ["col_a"]

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertFalse(result)

    def test_is_clone_replaceable_complex_matching_spec(self):
        """When tables have matching partition AND clustering, should return True"""
        template = self.__get_template("materializations/clone.sql")

        # Mock tables with both partitioning and clustering that match
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = mock.Mock()
        target_table.time_partitioning.field = "created_at"
        target_table.partitioning_type = "DAY"
        target_table.clustering_fields = ["user_id", "status"]

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = mock.Mock()
        source_table.time_partitioning.field = "created_at"
        source_table.partitioning_type = "DAY"
        source_table.clustering_fields = ["user_id", "status"]

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        self.assertTrue(result)

    def test_is_clone_replaceable_case_insensitive_field_names(self):
        """Partition field names should be compared case-insensitively"""
        template = self.__get_template("materializations/clone.sql")

        # Target uses "Created_At", source uses "created_at" (different case)
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = mock.Mock()
        target_table.time_partitioning.field = "Created_At"
        target_table.partitioning_type = "DAY"
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = mock.Mock()
        source_table.time_partitioning.field = "created_at"
        source_table.partitioning_type = "DAY"
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        # Should return True because field names match (case-insensitive)
        self.assertTrue(result)

    def test_is_clone_replaceable_clustering_none_vs_empty_list(self):
        """Clustering fields None and [] should be treated as equivalent"""
        template = self.__get_template("materializations/clone.sql")

        # Target has clustering_fields = None, source has clustering_fields = []
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = None
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = None
        source_table.clustering_fields = []

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        # Should return True because None and [] both mean "no clustering"
        self.assertTrue(result)

    def test_is_clone_replaceable_clustering_empty_list_vs_none(self):
        """Clustering fields [] and None should be treated as equivalent"""
        template = self.__get_template("materializations/clone.sql")

        # Target has clustering_fields = [], source has clustering_fields = None
        target_table = mock.Mock()
        target_table.range_partitioning = None
        target_table.time_partitioning = None
        target_table.clustering_fields = []

        source_table = mock.Mock()
        source_table.range_partitioning = None
        source_table.time_partitioning = None
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        # Should return True because [] and None both mean "no clustering"
        self.assertTrue(result)

    def test_is_clone_replaceable_range_partition_case_insensitive(self):
        """Range partition field names should be compared case-insensitively"""
        template = self.__get_template("materializations/clone.sql")

        # Target uses "ID", source uses "id" (different case)
        target_table = mock.Mock()
        target_table.time_partitioning = None
        target_table.range_partitioning = mock.Mock()
        target_table.range_partitioning.field = "ID"
        target_table.range_partitioning.range_ = mock.Mock()
        target_table.range_partitioning.range_.start = 1
        target_table.range_partitioning.range_.end = 100
        target_table.range_partitioning.range_.interval = 10
        target_table.clustering_fields = None

        source_table = mock.Mock()
        source_table.time_partitioning = None
        source_table.range_partitioning = mock.Mock()
        source_table.range_partitioning.field = "id"
        source_table.range_partitioning.range_ = mock.Mock()
        source_table.range_partitioning.range_.start = 1
        source_table.range_partitioning.range_.end = 100
        source_table.range_partitioning.range_.interval = 10
        source_table.clustering_fields = None

        self.mock_adapter.get_bq_table.side_effect = [target_table, source_table]

        result = self.__run_macro(
            template, "bigquery__is_clone_replaceable", mock.Mock(), mock.Mock()
        )

        # Should return True because field names match (case-insensitive)
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
