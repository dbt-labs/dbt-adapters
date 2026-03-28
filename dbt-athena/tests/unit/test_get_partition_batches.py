"""
Unit tests for the get_partition_batches macro.

Tests the full macro end-to-end using jinja2.FileSystemLoader with stubbed
dbt context, following the pattern used in the Spark and Snowflake adapters.
"""

import hashlib
import os
import re
from types import SimpleNamespace
from unittest import mock

import jinja2

# Directory containing the macro files
_HELPERS_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "materializations",
        "models",
        "helpers",
    )
)


class MockTable:
    """Mock for the dbt table object returned by load_result().table."""

    def __init__(self, rows, column_types):
        self.rows = rows
        self.column_types = column_types


class MockResult:
    """Mock for the dbt result returned by load_result()."""

    def __init__(self, table):
        self.table = table


def _deterministic_hash(value, num_buckets):
    """Deterministic hash function replacing adapter.murmur3_hash."""
    h = int(hashlib.md5(str(value).encode()).hexdigest(), 16)
    return h % num_buckets


def _format_partition_keys(partitioned_by):
    """Stub for adapter.format_partition_keys."""
    parts = []
    for key in partitioned_by:
        bucket_match = re.search(r"bucket\((.+?),\s*(\d+)\)", key)
        if bucket_match:
            parts.append(bucket_match.group(1))
        else:
            parts.append(f'"{key}"')
    return ", ".join(parts)


def _format_one_partition_key(key):
    """Stub for adapter.format_one_partition_key."""
    bucket_match = re.search(r"bucket\((.+?),\s*(\d+)\)", key)
    if bucket_match:
        return bucket_match.group(1)
    return f'"{key}"'


def _format_value_for_partition(value, column_type):
    """Stub for adapter.format_value_for_partition.

    Returns (formatted_value, comparison_operator).
    """
    if value is None:
        return ("''", " IS NULL -- ")
    if column_type == "date":
        return (f"DATE'{value}'", "=")
    if column_type == "integer" or column_type == "bigint":
        return (str(value), "=")
    return (f"'{value}'", "=")


def _render_macro(config, rows, column_types):
    """Load and render the full get_partition_batches macro with stubs.

    Args:
        config: Dict with keys like 'partitioned_by', 'partitions_limit'.
        rows: List of lists representing partition rows returned by the query.
        column_types: List of column type strings (e.g. ['date', 'varchar']).

    Returns:
        List of partition batch strings (WHERE clause fragments).
    """
    table = MockTable(rows, column_types)
    result_holder = {}

    def mock_convert_type(tbl, idx):
        return tbl.column_types[idx]

    # Build the context dict with all stubs
    context = {
        "config": mock.Mock(),
        "adapter": mock.Mock(),
        "statement": lambda *args, **kwargs: "",
        "load_result": lambda name: MockResult(table),
        "modules": SimpleNamespace(re=re),
        "zip": zip,
        "log": lambda *args, **kwargs: "",
        "return": lambda value: result_holder.update({"value": value}) or "",
    }

    context["config"].get = lambda key, *args, **kwargs: config.get(
        key, args[0] if args else kwargs.get("default")
    )
    context["adapter"].format_partition_keys = _format_partition_keys
    context["adapter"].format_one_partition_key = _format_one_partition_key
    context["adapter"].convert_type = mock_convert_type
    context["adapter"].format_value_for_partition = _format_value_for_partition
    context["adapter"].murmur3_hash = _deterministic_hash

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_HELPERS_DIR),
        extensions=["jinja2.ext.do"],
    )

    # Load process_bucket_column first so it's available in get_partition_batches
    pbc_template = env.get_template("process_bucket_column.sql", globals=context)
    context["process_bucket_column"] = pbc_template.module.process_bucket_column

    # Load and render the main macro
    template = env.get_template("get_partition_batches.sql", globals=context)
    template.module.get_partition_batches(sql="test")

    return result_holder["value"]


def _count_bucket_nums_in_batch(batch, partitioned_by):
    """Count distinct bucket numbers from IN clause values in a batch string.

    Used to verify that the actual number of Iceberg partitions opened
    (non_bucket_partitions × distinct_bucket_nums) respects the limit.
    """
    in_match = re.search(r"IN \((.+?)\)", batch)
    if not in_match:
        return 1
    values = [v.strip().strip("'") for v in in_match.group(1).split(", ")]
    for key in partitioned_by:
        bucket_match = re.search(r"bucket\((.+?),\s*(\d+)\)", key)
        if bucket_match:
            num_buckets = int(bucket_match.group(2))
            return len({_deterministic_hash(v, num_buckets) for v in values})
    return len(values)


class TestNonBucketedBatching:
    """Tests for the non-bucketed partition path."""

    def test_partitions_under_limit(self):
        result = _render_macro(
            config={"partitioned_by": ["date_col"], "partitions_limit": 100},
            rows=[["2024-01-01"], ["2024-01-02"], ["2024-01-03"]],
            column_types=["date"],
        )
        assert result == [
            "\"date_col\"=DATE'2024-01-01' or "
            "\"date_col\"=DATE'2024-01-02' or "
            "\"date_col\"=DATE'2024-01-03'",
        ]

    def test_partitions_exceeding_limit(self):
        result = _render_macro(
            config={"partitioned_by": ["date_col"], "partitions_limit": 2},
            rows=[[f"2024-01-{i:02d}"] for i in range(1, 6)],
            column_types=["date"],
        )
        assert result == [
            "\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02'",
            "\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04'",
            "\"date_col\"=DATE'2024-01-05'",
        ]

    def test_multi_column_partition(self):
        result = _render_macro(
            config={"partitioned_by": ["date_col", "region"], "partitions_limit": 100},
            rows=[
                ["2024-01-01", "us-east-1"],
                ["2024-01-01", "eu-west-1"],
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "\"date_col\"=DATE'2024-01-01' and \"region\"='us-east-1' or "
            "\"date_col\"=DATE'2024-01-01' and \"region\"='eu-west-1'",
        ]


class TestPartitionsLimitClamping:
    """Tests verifying that partitions_limit is clamped to a minimum of 1."""

    def test_limit_zero_non_bucketed(self):
        """partitions_limit=0 should be clamped to 1, producing one partition per batch."""
        result = _render_macro(
            config={"partitioned_by": ["date_col"], "partitions_limit": 0},
            rows=[["2024-01-01"], ["2024-01-02"], ["2024-01-03"]],
            column_types=["date"],
        )
        assert result == [
            "\"date_col\"=DATE'2024-01-01'",
            "\"date_col\"=DATE'2024-01-02'",
            "\"date_col\"=DATE'2024-01-03'",
        ]

    def test_negative_limit_non_bucketed(self):
        """Negative partitions_limit should be clamped to 1."""
        result = _render_macro(
            config={"partitioned_by": ["date_col"], "partitions_limit": -5},
            rows=[["2024-01-01"], ["2024-01-02"]],
            column_types=["date"],
        )
        assert result == [
            "\"date_col\"=DATE'2024-01-01'",
            "\"date_col\"=DATE'2024-01-02'",
        ]

    def test_limit_zero_bucketed(self):
        """partitions_limit=0 with buckets should be clamped to 1."""
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 2)"],
                "partitions_limit": 0,
            },
            rows=[["a"], ["b"], ["c"]],
            column_types=["varchar"],
        )
        assert result == [
            "col IN ('a')",
            "col IN ('b')",
            "col IN ('c')",
        ]

    def test_limit_zero_bucketed_with_partitions(self):
        """partitions_limit=0 with bucket + non-bucket columns should be clamped to 1."""
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": 0,
            },
            rows=[
                ["2024-01-01", "alice"],
                ["2024-01-02", "bob"],
            ],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            assert num_or == 1


class TestBucketOnlyBatching:
    """Tests for bucket partitioning without non-bucket partition columns."""

    def test_bucket_only_values_chunked(self):
        """When all bucket numbers fit within the limit, both buckets are grouped.

        md5 hash with bucket(col, 2): bucket 0=['e'], bucket 1=['a','b','c','d'].
        limit=2 allows grouping both buckets (2 ≤ 2).
        Values are still chunked by athena_partitions_limit to avoid query size issues.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 2)"],
                "partitions_limit": 2,
            },
            rows=[["a"], ["b"], ["c"], ["d"], ["e"]],
            column_types=["varchar"],
        )
        assert result == [
            "col IN ('a', 'b')",
            "col IN ('c', 'd')",
            "col IN ('e')",
        ]

    def test_bucket_only_no_chunking(self):
        """Bucket values within limit should not be split.

        md5 hash with bucket(col, 2): bucket 0=['e'], bucket 1=['a','b','c','d'].
        limit=10, both buckets grouped together.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 2)"],
                "partitions_limit": 10,
            },
            rows=[["a"], ["b"], ["c"], ["d"], ["e"]],
            column_types=["varchar"],
        )
        assert result == [
            "col IN ('a', 'b', 'c', 'd', 'e')",
        ]

    def test_bucket_only_limit_one(self):
        """Bucket-only with limit=1: each bucket number becomes its own batch.

        md5 hash with bucket(col, 2): bucket 0=['e'], bucket 1=['a','b','c','d'].
        limit=1 forces 1 bucket per batch. Values are also chunked by limit=1,
        so each value gets its own batch.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 2)"],
                "partitions_limit": 1,
            },
            rows=[["a"], ["b"], ["c"], ["d"], ["e"]],
            column_types=["varchar"],
        )
        assert result == [
            "col IN ('a')",
            "col IN ('b')",
            "col IN ('c')",
            "col IN ('d')",
            "col IN ('e')",
        ]

    def test_bucket_only_many_buckets_exceed_limit(self):
        """When there are more bucket numbers than the limit, they are grouped.

        bucket(col, 5) with 5 values spread across multiple buckets.
        limit=2 groups up to 2 bucket numbers per batch.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 5)"],
                "partitions_limit": 2,
            },
            rows=[["a"], ["b"], ["c"], ["d"], ["e"]],
            column_types=["varchar"],
        )
        assert result == [
            "col IN ('a', 'b')",
            "col IN ('c', 'd')",
            "col IN ('e')",
        ]


class TestBucketWithPartitionsBatching:
    """Tests for bucket + non-bucket partition columns (AND structure)."""

    def test_single_partition_with_bucket(self):
        """Single partition value combined with bucket.

        Bucket numbers are grouped: alice→bucket 0, bob→bucket 2.
        limit=100, partition_chunk=1, bucket_chunk=100.
        Both buckets fit in one group.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 10)"],
                "partitions_limit": 100,
            },
            rows=[
                ["2024-01-01", "alice"],
                ["2024-01-01", "bob"],
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('alice', 'bob')",
        ]

    def test_multiple_partitions_with_bucket(self):
        """Multiple partition values (under limit) combined with grouped buckets.

        alice→bucket 0, bob→bucket 2, charlie→bucket 3. All 3 buckets grouped.
        limit=100, partition_chunk=2, bucket_chunk=50.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 5)"],
                "partitions_limit": 100,
            },
            rows=[
                ["2024-01-01", "alice"],
                ["2024-01-01", "bob"],
                ["2024-01-02", "charlie"],
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('alice', 'bob', 'charlie')",
        ]

    def test_single_partition_with_many_users_few_buckets(self):
        """non_bucket=1, many users but few actual bucket numbers.

        With bucket(user_id, 2), all users land in just 2 buckets.
        limit=3, partition_chunk=1, bucket_chunk=3.
        Both buckets fit in one group, values chunked by limit=3.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": 3,
            },
            rows=[["2024-01-01", f"user{i}"] for i in range(1, 8)],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('user1', 'user6', 'user7')",
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('user2', 'user3', 'user4')",
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('user5')",
        ]

    def test_few_partitions_with_many_bucket_values(self):
        """non_bucket=2, limit=4 → partition_chunk=2, bucket_chunk=2.

        With bucket(user_id, 2), both bucket numbers fit in one group.
        Values chunked by limit=4.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": 4,
            },
            rows=[
                [date, f"user{i}"] for date in ["2024-01-01", "2024-01-02"] for i in range(1, 8)
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user1', 'user6', 'user7', 'user2')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user3', 'user4', 'user5')",
        ]

    def test_partitions_split_with_buckets(self):
        """Partitions exceeding the limit should be split, each combined with grouped buckets.

        With coordinated chunk sizes (limit=2, 5 non-bucket partitions):
          partition_chunk_size = min(5, 2) = 2
          bucket_chunk_size = max(1, floor(2/2)) = 1
        So each bucket number gets its own group.

        md5 hash with bucket(user_id, 3):
          user1→bucket 1, user2→bucket 2, user3→bucket 2,
          user4→bucket 0, user5→bucket 2
        Bucket 2 has 3 values (user2, user3, user5), but values are also chunked
        by athena_partitions_limit=2, so they are split into (user2,user3) and (user5).
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 3)"],
                "partitions_limit": 2,
            },
            rows=[[f"2024-01-{i:02d}", f"user{i}"] for i in range(1, 6)],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user1')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user1')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user1')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user2', 'user3')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user2', 'user3')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user2', 'user3')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user5')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user5')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user5')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user4')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user4')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user4')",
        ]


class TestCrossProductLimit:
    """Tests verifying that the cross-product of partitions × bucket numbers respects the limit.

    The key insight: values within the same bucket share one Iceberg partition,
    so the open partition count is non_bucket_partitions × distinct_bucket_nums,
    NOT non_bucket_partitions × num_values_in_IN_clause.
    """

    def test_cross_product_respects_limit(self):
        """Invariant: every batch's partition × bucket_nums must be ≤ partitions_limit.

        Expected output (9 batches, abbreviated):
          (date=01 or ... or date=04) and user_id IN ('user1', 'user6', ...)
          (date=05 or ... or date=08) and user_id IN ('user1', 'user6', ...)
          (date=09 or date=10)        and user_id IN ('user1', 'user6', ...)
          ... repeated for each bucket group (3 groups total) ...
        """
        limit = 4
        partitioned_by = ["date_col", "bucket(user_id, 3)"]
        result = _render_macro(
            config={
                "partitioned_by": partitioned_by,
                "partitions_limit": limit,
            },
            rows=[[f"2024-01-{i:02d}", f"user{i}"] for i in range(1, 11)],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            num_buckets = _count_bucket_nums_in_batch(batch, partitioned_by)
            assert num_or * num_buckets <= limit, (
                f"Batch exceeds limit {limit}: "
                f"{num_or} partitions × {num_buckets} buckets = {num_or * num_buckets}"
            )

    def test_production_scale_respects_limit(self):
        """Production-like scenario: 12 months × many users, limit=100.

        partition_chunk=12, bucket_chunk=floor(100/12)=8.
        Bucket numbers are grouped by 8.

        Expected output (abbreviated):
          (date=2024-01 or ... or date=2024-12) and user_id IN (bucket group 0..7 values)
          (date=2024-01 or ... or date=2024-12) and user_id IN (bucket group 8..9 values)
        """
        limit = 100
        partitioned_by = ["date_col", "bucket(user_id, 10)"]
        months = [f"2024-{m:02d}-01" for m in range(1, 13)]
        users = [f"user{i}" for i in range(1, 51)]
        rows = [[month, user] for month in months for user in users]
        result = _render_macro(
            config={
                "partitioned_by": partitioned_by,
                "partitions_limit": limit,
            },
            rows=rows,
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            num_buckets = _count_bucket_nums_in_batch(batch, partitioned_by)
            assert num_or * num_buckets <= limit, (
                f"Batch exceeds limit {limit}: "
                f"{num_or} partitions × {num_buckets} buckets = {num_or * num_buckets}"
            )

    def test_non_bucket_count_exceeds_limit(self):
        """When non-bucket partitions exceed limit, partition_chunk_size = limit, bucket_chunk_size = 1.

        Expected output (9 batches, abbreviated):
          (date=01 or date=02 or date=03) and user_id IN ('u1', 'u3', 'u4')
          (date=04 or date=05 or date=06) and user_id IN ('u1', 'u3', 'u4')
          (date=07)                        and user_id IN ('u1', 'u3', 'u4')
          ... repeated for each bucket (3 buckets, 1 per group) ...
        """
        limit = 3
        partitioned_by = ["date_col", "bucket(user_id, 2)"]
        result = _render_macro(
            config={
                "partitioned_by": partitioned_by,
                "partitions_limit": limit,
            },
            rows=[[f"2024-01-{i:02d}", f"u{i}"] for i in range(1, 8)],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            num_buckets = _count_bucket_nums_in_batch(batch, partitioned_by)
            assert num_or * num_buckets <= limit, (
                f"Batch exceeds limit {limit}: "
                f"{num_or} partitions × {num_buckets} buckets = {num_or * num_buckets}"
            )

    def test_non_bucket_count_equals_limit(self):
        """When non-bucket count == limit, partition_chunk = limit, bucket_chunk = 1."""
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": 3,
            },
            rows=[[f"2024-01-{i:02d}", f"u{i}"] for i in range(1, 4)],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02' or \"date_col\"=DATE'2024-01-03') and user_id IN ('u1', 'u3')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02' or \"date_col\"=DATE'2024-01-03') and user_id IN ('u2')",
        ]

    def test_limit_one(self):
        """Extreme edge case: limit=1 forces partition_chunk=1 and bucket_chunk=1."""
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": 1,
            },
            rows=[
                ["2024-01-01", "alice"],
                ["2024-01-02", "bob"],
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-02') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-02') and user_id IN ('bob')",
        ]


class TestCompleteness:
    """Tests verifying all data is covered when limit is very small."""

    def test_small_limit_covers_all_data(self):
        """limit=2 with 3 dates × 3 users: every date-user combination must appear.

        partition_chunk_size = min(3, 2) = 2, bucket_chunk_size = max(1, floor(2/2)) = 1.
        md5 hash with bucket(user_id, 3): alice→0, bob→2, charlie→1.
        Each bucket number gets its own group.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 3)"],
                "partitions_limit": 2,
            },
            rows=[
                [d, u]
                for d in ["2024-01-01", "2024-01-02", "2024-01-03"]
                for u in ["alice", "bob", "charlie"]
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-03') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-03') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('charlie')",
            "(\"date_col\"=DATE'2024-01-03') and user_id IN ('charlie')",
        ]

    def test_limit_one_covers_all_data(self):
        """limit=1 with 3 dates × 3 users: each batch is exactly 1 date × 1 bucket group.

        partition_chunk_size = min(3, 1) = 1, bucket_chunk_size = max(1, floor(1/1)) = 1.
        md5 hash with bucket(user_id, 5): alice→0, bob→4, charlie→3.
        Each bucket number gets its own group, combined with each date.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 5)"],
                "partitions_limit": 1,
            },
            rows=[
                [d, u]
                for d in ["2024-01-01", "2024-01-02", "2024-01-03"]
                for u in ["alice", "bob", "charlie"]
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-02') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-03') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-02') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-03') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('charlie')",
            "(\"date_col\"=DATE'2024-01-02') and user_id IN ('charlie')",
            "(\"date_col\"=DATE'2024-01-03') and user_id IN ('charlie')",
        ]


class TestBatchCountReduction:
    """Tests verifying that grouping bucket numbers reduces the total batch count."""

    def test_grouping_reduces_batches(self):
        """With 3 bucket numbers and bucket_chunk=3, all combine into 1 group.

        Without grouping: 3 buckets × 1 partition_batch = 3 batches.
        With grouping: 1 group × 1 partition_batch = 1 batch.
        """
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 5)"],
                "partitions_limit": 100,
            },
            rows=[
                ["2024-01-01", "alice"],
                ["2024-01-01", "bob"],
                ["2024-01-02", "charlie"],
            ],
            column_types=["date", "varchar"],
        )
        assert result == [
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('alice', 'bob', 'charlie')",
        ]

    def test_large_scale_respects_limit_and_reduces_batches(self):
        """12 years × 512 buckets with limit=100.

        partition_chunk=12, bucket_chunk=floor(100/12)=8.
        512 bucket_nums grouped by 8 → ceil(512/8) = 64 groups.
        64 groups × 1 partition_batch = 64 batches (vs 512 without grouping).

        Expected output (abbreviated):
          (date=2014 or ... or date=2025) and user_id IN (bucket group 0..7 values)
          (date=2014 or ... or date=2025) and user_id IN (bucket group 8..15 values)
          ... ~64 batches total, each ≤ 12 dates × 8 bucket nums = 96 ≤ 100 ...
        """
        limit = 100
        partitioned_by = ["date_col", "bucket(user_id, 512)"]
        years = [f"{y}-01-01" for y in range(2014, 2026)]
        users = [f"user{i}" for i in range(1, 201)]
        rows = [[year, user] for year in years for user in users]
        result = _render_macro(
            config={
                "partitioned_by": partitioned_by,
                "partitions_limit": limit,
            },
            rows=rows,
            column_types=["date", "varchar"],
        )
        # Verify limit is respected
        for batch in result:
            num_or = batch.count(" or ") + 1
            num_buckets = _count_bucket_nums_in_batch(batch, partitioned_by)
            assert num_or * num_buckets <= limit, (
                f"Batch exceeds limit {limit}: "
                f"{num_or} partitions × {num_buckets} buckets = {num_or * num_buckets}"
            )
        # With grouping, batch count should be much less than 200
        # (number of distinct bucket_nums for 200 users across 512 buckets)
        assert len(result) < 200
