"""Unit tests for SnowflakeColumn collation support."""

import unittest

from dbt.adapters.snowflake.column import SnowflakeColumn


class TestSnowflakeColumnCollation(unittest.TestCase):
    """Test cases for collation parsing and generation in SnowflakeColumn."""

    def test_parse_varchar_with_collation(self):
        """Test parsing VARCHAR with COLLATE clause."""
        column = SnowflakeColumn.from_description(
            "test_col", "VARCHAR(16777216) COLLATE 'en-ci-rtrim'"
        )

        assert column.name == "test_col"
        assert column.dtype == "VARCHAR"
        assert column.char_size == 16777216
        assert column.collation == "en-ci-rtrim"

    def test_parse_varchar_without_collation(self):
        """Test parsing VARCHAR without COLLATE clause."""
        column = SnowflakeColumn.from_description("test_col", "VARCHAR(100)")

        assert column.name == "test_col"
        assert column.dtype == "VARCHAR"
        assert column.char_size == 100
        assert column.collation is None

    def test_parse_text_with_collation(self):
        """Test parsing TEXT with COLLATE clause."""
        column = SnowflakeColumn.from_description("test_col", "TEXT COLLATE 'es-ai'")

        assert column.name == "test_col"
        assert column.dtype == "TEXT COLLATE 'es-ai'"
        assert column.collation == "es-ai"

    def test_parse_character_varying_with_collation(self):
        """Test parsing CHARACTER VARYING with COLLATE clause."""
        column = SnowflakeColumn.from_description(
            "test_col", "CHARACTER VARYING(1000) COLLATE 'fr'"
        )

        assert column.name == "test_col"
        assert column.dtype == "CHARACTER VARYING"
        assert column.char_size == 1000
        assert column.collation == "fr"

    def test_parse_number_without_collation(self):
        """Test parsing NUMBER (non-string type) has no collation."""
        column = SnowflakeColumn.from_description("test_col", "NUMBER(38,0)")

        assert column.name == "test_col"
        assert column.dtype == "NUMBER"
        assert column.numeric_precision == 38
        assert column.numeric_scale == 0
        assert column.collation is None

    def test_data_type_with_collation(self):
        """Test that data_type property includes collation for strings."""
        column = SnowflakeColumn.from_description(
            "test_col", "VARCHAR(16777216) COLLATE 'en-ci-rtrim'"
        )

        # data_type should include collation
        expected = "character varying(16777216) COLLATE 'en-ci-rtrim'"
        assert column.data_type == expected

    def test_data_type_without_collation(self):
        """Test that data_type property works without collation."""
        column = SnowflakeColumn.from_description("test_col", "VARCHAR(100)")

        # data_type should not include collation
        expected = "character varying(100)"
        assert column.data_type == expected

    def test_data_type_number_ignores_collation(self):
        """Test that non-string types don't include collation even if set."""
        column = SnowflakeColumn.from_description("test_col", "NUMBER(10,2)")
        # Manually set collation (shouldn't happen in practice)
        column.collation = "en-ci"

        # data_type should not include collation for non-string types
        expected = "NUMBER(10,2)"
        assert column.data_type == expected

    def test_parse_collation_case_insensitive(self):
        """Test that COLLATE keyword is case-insensitive."""
        # Test uppercase
        column1 = SnowflakeColumn.from_description("test_col", "VARCHAR(100) COLLATE 'en-ci'")
        assert column1.collation == "en-ci"

        # Test lowercase
        column2 = SnowflakeColumn.from_description("test_col", "VARCHAR(100) collate 'en-ci'")
        assert column2.collation == "en-ci"

        # Test mixed case
        column3 = SnowflakeColumn.from_description("test_col", "VARCHAR(100) CoLLaTe 'en-ci'")
        assert column3.collation == "en-ci"

    def test_parse_varchar_max_size_with_collation(self):
        """Test parsing VARCHAR with maximum size (134217728) and collation."""
        column = SnowflakeColumn.from_description(
            "test_col", "VARCHAR(134217728) COLLATE 'en-ci-rtrim'"
        )

        assert column.name == "test_col"
        assert column.dtype == "VARCHAR"
        assert column.char_size == 134217728
        assert column.collation == "en-ci-rtrim"

    def test_data_type_preserves_collation_after_size_change(self):
        """Test that collation is preserved when generating data type."""
        # This simulates what happens during schema change detection
        column = SnowflakeColumn.from_description(
            "test_col", "VARCHAR(16777216) COLLATE 'en-ci-rtrim'"
        )

        # When dbt detects a size change, it should preserve collation
        assert "COLLATE 'en-ci-rtrim'" in column.data_type

    def test_parse_text_without_size_with_collation(self):
        """Test parsing TEXT (no size) with collation."""
        column = SnowflakeColumn.from_description("test_col", "TEXT COLLATE 'utf8'")

        assert column.name == "test_col"
        assert column.dtype == "TEXT COLLATE 'utf8'"
        assert column.char_size is None
        assert column.collation == "utf8"
        assert "COLLATE 'utf8'" in column.data_type

    def test_parse_array_type_no_collation(self):
        """Test that ARRAY types don't have collation parsing."""
        column = SnowflakeColumn.from_description("test_col", "ARRAY")

        assert column.name == "test_col"
        assert column.dtype == "ARRAY"
        assert column.collation is None

    def test_parse_object_type_no_collation(self):
        """Test that OBJECT types don't have collation parsing."""
        column = SnowflakeColumn.from_description("test_col", "OBJECT")

        assert column.name == "test_col"
        assert column.dtype == "OBJECT"
        assert column.collation is None


if __name__ == "__main__":
    unittest.main()
