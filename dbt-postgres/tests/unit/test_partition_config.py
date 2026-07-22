import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.postgres.partitioning import PostgresPartitionConfig


class TestPostgresPartitionConfigParse:
    def test_none_returns_none(self):
        assert PostgresPartitionConfig.parse(None) is None

    def test_minimal_range(self):
        cfg = PostgresPartitionConfig.parse({"fields": ["created_at"]})
        assert cfg.fields == ["created_at"]
        assert cfg.method == "range"  # default
        assert cfg.default_partition is True  # default
        assert cfg.render == "range (created_at)"

    def test_multi_column_key(self):
        cfg = PostgresPartitionConfig.parse(
            {"fields": ["tenant_id", "created_at"], "method": "range"}
        )
        assert cfg.render == "range (tenant_id, created_at)"

    def test_list_with_partitions(self):
        cfg = PostgresPartitionConfig.parse(
            {
                "fields": ["region"],
                "method": "list",
                "partitions": [{"name": "p_us", "values": ["'us'"]}],
            }
        )
        assert cfg.method == "list"
        assert cfg.partitions[0]["name"] == "p_us"

    def test_hash_method(self):
        cfg = PostgresPartitionConfig.parse({"fields": ["id"], "method": "hash"})
        assert cfg.render == "hash (id)"

    def test_invalid_method_raises(self):
        with pytest.raises(DbtRuntimeError, match="method"):
            PostgresPartitionConfig.parse({"fields": ["id"], "method": "nonsense"})

    def test_non_dict_raises(self):
        with pytest.raises(DbtRuntimeError, match="partition_by"):
            PostgresPartitionConfig.parse("created_at")

    def test_empty_fields_raises(self):
        with pytest.raises(DbtRuntimeError, match="at least one column"):
            PostgresPartitionConfig.parse({"fields": [], "method": "range"})

    def test_granularity_requires_range(self):
        with pytest.raises(DbtRuntimeError, match="granularity"):
            PostgresPartitionConfig.parse(
                {"fields": ["id"], "method": "hash", "granularity": "day"}
            )

    def test_invalid_granularity_raises(self):
        with pytest.raises(DbtRuntimeError, match="granularity"):
            PostgresPartitionConfig.parse(
                {"fields": ["created_at"], "method": "range", "granularity": "fortnight"}
            )
