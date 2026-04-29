import pytest

from dbt.adapters.snowflake.impl import SnowflakeConfig


class TestCopyTagsConfig:
    def test_copy_tags_defaults_to_none(self):
        config = SnowflakeConfig()
        assert config.copy_tags is None

    def test_copy_tags_accepts_true(self):
        config = SnowflakeConfig(copy_tags=True)
        assert config.copy_tags is True

    def test_copy_tags_accepts_false(self):
        config = SnowflakeConfig(copy_tags=False)
        assert config.copy_tags is False

    def test_copy_tags_independent_of_copy_grants(self):
        config = SnowflakeConfig(copy_grants=True, copy_tags=False)
        assert config.copy_grants is True
        assert config.copy_tags is False

        config = SnowflakeConfig(copy_grants=False, copy_tags=True)
        assert config.copy_grants is False
        assert config.copy_tags is True

    def test_copy_tags_and_copy_grants_both_enabled(self):
        config = SnowflakeConfig(copy_grants=True, copy_tags=True)
        assert config.copy_grants is True
        assert config.copy_tags is True
