from datetime import datetime

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.postgres.partitioning import (
    MAX_AUTO_PARTITIONS,
    PostgresPartitionConfig,
    compute_partition_bounds,
)


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


class TestComputePartitionBounds:
    def test_none_bounds_return_empty(self):
        assert compute_partition_bounds(None, None, "month") == []

    def test_monthly_bounds(self):
        bounds = compute_partition_bounds(datetime(2024, 1, 15), datetime(2024, 3, 5), "month")
        assert [b["name"] for b in bounds] == ["p202401", "p202402", "p202403"]
        assert bounds[0]["from"] == "'2024-01-01 00:00:00'"
        assert bounds[0]["to"] == "'2024-02-01 00:00:00'"

    def test_accepts_date_and_iso_string(self):
        from datetime import date

        assert len(compute_partition_bounds(date(2024, 1, 1), date(2024, 1, 1), "day")) == 1
        assert len(compute_partition_bounds("2024-01-01", "2024-01-02", "day")) == 2

    def test_cap_raises(self):
        # hourly across many years blows past the cap
        with pytest.raises(DbtRuntimeError, match=str(MAX_AUTO_PARTITIONS)):
            compute_partition_bounds(datetime(2000, 1, 1), datetime(2020, 1, 1), "hour")
