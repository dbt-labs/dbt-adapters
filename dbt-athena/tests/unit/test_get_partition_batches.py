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


class TestBucketOnlyBatching:
    """Tests for bucket partitioning without non-bucket partition columns."""

    def test_bucket_only_values_chunked(self):
        """Bucket values exceeding athena_partitions_limit should be chunked."""
        # md5 hash with bucket(col, 2): bucket 0=['e'], bucket 1=['a','b','c','d']
        # With limit=2, bucket 1 is split into ['a','b'] and ['c','d']
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

    def test_bucket_only_values_under_limit(self):
        """Bucket values within limit should not be split."""
        # md5 hash with bucket(col, 2): bucket 0=['e'], bucket 1=['a','b','c','d']
        # With limit=10, no splitting needed
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 2)"],
                "partitions_limit": 10,
            },
            rows=[["a"], ["b"], ["c"], ["d"], ["e"]],
            column_types=["varchar"],
        )
        assert result == [
            "col IN ('a', 'b', 'c', 'd')",
            "col IN ('e')",
        ]

    def test_bucket_only_limit_one(self):
        """Bucket-only with limit=1: each value becomes its own batch."""
        result = _render_macro(
            config={
                "partitioned_by": ["bucket(col, 2)"],
                "partitions_limit": 1,
            },
            rows=[["a"], ["b"], ["c"], ["d"], ["e"]],
            column_types=["varchar"],
        )
        for batch in result:
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", "))
            assert num_in <= 1, f"Batch has {num_in} values, expected ≤ 1"
        assert len(result) == 5


class TestBucketWithPartitionsBatching:
    """Tests for bucket + non-bucket partition columns (AND structure)."""

    def test_single_partition_with_bucket(self):
        """Single partition value combined with bucket produces (partition) AND col IN (...)."""
        # md5 hash: alice->bucket 0, bob->bucket 2
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
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-01') and user_id IN ('bob')",
        ]

    def test_multiple_partitions_with_bucket(self):
        """Multiple partition values (under limit) combined with buckets."""
        # md5 hash with bucket(user_id, 5): alice->0, bob->2, charlie->3
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
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('alice')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('bob')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('charlie')",
        ]

    def test_single_partition_with_bucket_values_exceeding_limit(self):
        """non_bucket=1, bucket values > limit.

        partition_chunk = min(1, 3) = 1, bucket_chunk = floor(3/1) = 3.
        Bucket values exceeding 3 should be split into chunks of 3.
        """
        limit = 3
        # 7 users on a single date — hash distributes across buckets
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": limit,
            },
            rows=[["2024-01-01", f"user{i}"] for i in range(1, 8)],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} × {num_in} = {num_or * num_in}"
            )
            assert num_or == 1, "Single non-bucket partition should not be split"
            assert num_in <= limit, f"Bucket chunk should be ≤ {limit}"

    def test_few_partitions_with_many_bucket_values(self):
        """non_bucket < limit, bucket values > bucket_chunk.

        non_bucket=2, limit=4 → partition_chunk=2, bucket_chunk=floor(4/2)=2.
        A bucket with 5 values should be split into chunks of 2.
        """
        limit = 4
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": limit,
            },
            rows=[
                [date, f"user{i}"]
                for date in ["2024-01-01", "2024-01-02"]
                for i in range(1, 8)
            ],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} × {num_in} = {num_or * num_in}"
            )
            assert num_in <= 2, f"Bucket chunk should be ≤ 2 (floor({limit}/2))"

    def test_partitions_split_with_buckets(self):
        """Partitions exceeding the limit should be split, each combined with each bucket.

        With coordinated chunk sizes (limit=2, 5 non-bucket partitions):
          partition_chunk_size = min(5, 2) = 2
          bucket_chunk_size = max(1, floor(2/2)) = 1
        So bucket values are chunked individually to keep cross-product ≤ limit.

        md5 hash with bucket(user_id, 3):
          user1->bucket 1, user2->bucket 2, user3->bucket 2,
          user4->bucket 0, user5->bucket 2
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
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user2')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user2')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user2')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user3')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user3')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user3')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user5')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user5')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user5')",
            "(\"date_col\"=DATE'2024-01-01' or \"date_col\"=DATE'2024-01-02') and user_id IN ('user4')",
            "(\"date_col\"=DATE'2024-01-03' or \"date_col\"=DATE'2024-01-04') and user_id IN ('user4')",
            "(\"date_col\"=DATE'2024-01-05') and user_id IN ('user4')",
        ]


class TestCrossProductLimit:
    """Tests verifying that the cross-product of partitions × bucket values respects the limit."""

    def test_cross_product_respects_limit(self):
        """Invariant: every batch's cross-product must be ≤ partitions_limit."""
        limit = 4
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 3)"],
                "partitions_limit": limit,
            },
            rows=[[f"2024-01-{i:02d}", f"user{i}"] for i in range(1, 11)],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} partitions × {num_in} bucket values = {num_or * num_in}"
            )

    def test_many_months_few_buckets(self):
        """Production-like scenario: 12 months × many users, limit=100."""
        limit = 100
        months = [f"2024-{m:02d}-01" for m in range(1, 13)]
        users = [f"user{i}" for i in range(1, 51)]
        rows = [[month, user] for month in months for user in users]
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 10)"],
                "partitions_limit": limit,
            },
            rows=rows,
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} partitions × {num_in} bucket values = {num_or * num_in}"
            )

    def test_non_bucket_count_exceeds_limit(self):
        """When non-bucket partitions exceed limit, partition_chunk_size = limit, bucket_chunk_size = 1."""
        limit = 3
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": limit,
            },
            rows=[[f"2024-01-{i:02d}", f"u{i}"] for i in range(1, 8)],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} partitions × {num_in} bucket values = {num_or * num_in}"
            )

    def test_non_bucket_count_equals_limit(self):
        """When non-bucket count == limit, partition_chunk = limit, bucket_chunk = 1."""
        limit = 3
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": limit,
            },
            rows=[[f"2024-01-{i:02d}", f"u{i}"] for i in range(1, 4)],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} partitions × {num_in} bucket values = {num_or * num_in}"
            )
        assert all("(" in b and "IN" in b for b in result)

    def test_limit_one(self):
        """Extreme edge case: limit=1 forces partition_chunk=1 and bucket_chunk=1."""
        limit = 1
        result = _render_macro(
            config={
                "partitioned_by": ["date_col", "bucket(user_id, 2)"],
                "partitions_limit": limit,
            },
            rows=[
                ["2024-01-01", "alice"],
                ["2024-01-02", "bob"],
            ],
            column_types=["date", "varchar"],
        )
        for batch in result:
            num_or = batch.count(" or ") + 1
            in_match = re.search(r"IN \((.+?)\)", batch)
            num_in = len(in_match.group(1).split(", ")) if in_match else 1
            assert num_or * num_in <= limit, (
                f"Batch exceeds limit {limit}: {num_or} partitions × {num_in} bucket values = {num_or * num_in}"
            )
        assert len(result) > 0


class TestCompleteness:
    """Tests verifying all data is covered when limit is very small."""

    def test_small_limit_covers_all_data(self):
        """limit=2 with 3 dates × 3 users: every date-user combination must appear.

        partition_chunk_size = min(3, 2) = 2, bucket_chunk_size = max(1, floor(2/2)) = 1.
        md5 hash with bucket(user_id, 3): alice->0, bob->2, charlie->1.
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
        """limit=1 with 3 dates × 3 users: each batch is exactly 1 date × 1 user.

        partition_chunk_size = min(3, 1) = 1, bucket_chunk_size = max(1, floor(1/1)) = 1.
        md5 hash with bucket(user_id, 5): alice->0, bob->4, charlie->3.
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

    def test_bucket_only_limit_one_covers_all_data(self):
        """Bucket-only with limit=1: each value becomes its own batch.

        md5 hash with bucket(col, 2): bucket 0=['e'], bucket 1=['a','b','c','d'].
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
