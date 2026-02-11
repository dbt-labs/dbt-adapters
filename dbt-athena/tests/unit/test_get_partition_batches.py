"""
Unit tests for the batch-building logic in get_partition_batches.sql macro.

Since the macro relies heavily on dbt context (config, adapter, statement, etc.),
we extract and test the batch construction logic (lines 52-97) in isolation
using Jinja2 directly. This validates the core splitting algorithm without
requiring a full dbt rendering environment.
"""

import jinja2
import pytest

# Jinja2 template that mirrors the batch-building logic from get_partition_batches.sql.
# Inputs: partitions, bucket_conditions, bucket_numbers, bucket_column, is_bucketed,
#         athena_partitions_limit
BATCH_BUILDING_TEMPLATE = """\
{%- set ns_partitions = partitions -%}
{%- set ns_is_bucketed = is_bucketed -%}
{%- set ns_bucket_conditions = bucket_conditions -%}
{%- set ns_bucket_numbers = bucket_numbers -%}
{%- set ns_bucket_column = bucket_column -%}

{%- set partitions_batches = [] -%}
{%- if ns_is_bucketed -%}
    {%- set max_in_bytes = max_in_bytes_override if max_in_bytes_override is defined else 200000 -%}

    {%- set partition_batches = [] -%}
    {%- set non_empty_partitions = ns_partitions | select | list -%}
    {%- if non_empty_partitions | length > 0 -%}
        {%- for i in range(0, non_empty_partitions | length, athena_partitions_limit) -%}
            {%- set batch = non_empty_partitions[i:i + athena_partitions_limit] -%}
            {%- do partition_batches.append(batch | join(' or ')) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- for bucket_num in ns_bucket_numbers -%}
        {%- set values = ns_bucket_conditions[bucket_num] -%}
        {%- set values_total_len = values | join(', ') | length -%}
        {%- if values_total_len > max_in_bytes -%}
            {%- set num_chunks = (values_total_len // max_in_bytes) + (1 if values_total_len % max_in_bytes > 0 else 0) -%}
        {%- else -%}
            {%- set num_chunks = 1 -%}
        {%- endif -%}
        {%- set chunk_size = (values | length // num_chunks) + (1 if values | length % num_chunks > 0 else 0) -%}

        {%- for ci in range(num_chunks) -%}
            {%- set chunk = values[ci * chunk_size : (ci + 1) * chunk_size] -%}
            {%- set bucket_cond = ns_bucket_column ~ " IN (" ~ chunk | join(", ") ~ ")" -%}

            {%- if partition_batches | length > 0 -%}
                {%- for pb in partition_batches -%}
                    {%- do partitions_batches.append("(" ~ pb ~ ") and " ~ bucket_cond) -%}
                {%- endfor -%}
            {%- else -%}
                {%- do partitions_batches.append(bucket_cond) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
{%- else -%}
    {%- for i in range(0, ns_partitions | length, athena_partitions_limit) -%}
        {%- set batch = ns_partitions[i:i + athena_partitions_limit] -%}
        {%- do partitions_batches.append(batch | join(' or ')) -%}
    {%- endfor -%}
{%- endif -%}
{{ partitions_batches | tojson }}
"""


def _render_batches(**kwargs):
    """Render the batch-building template and return the resulting list."""
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    template = env.from_string(BATCH_BUILDING_TEMPLATE)
    import json

    rendered = template.render(**kwargs).strip()
    return json.loads(rendered)


class TestNonBucketedBatching:
    """Tests for the non-bucketed partition path (existing behavior)."""

    def test_single_partition(self):
        result = _render_batches(
            partitions=["date_col='2024-01-01'"],
            is_bucketed=False,
            bucket_conditions={},
            bucket_numbers=[],
            bucket_column=None,
            athena_partitions_limit=100,
        )
        assert result == ["date_col='2024-01-01'"]

    def test_partitions_under_limit(self):
        partitions = [f"date_col='2024-01-{i:02d}'" for i in range(1, 4)]
        result = _render_batches(
            partitions=partitions,
            is_bucketed=False,
            bucket_conditions={},
            bucket_numbers=[],
            bucket_column=None,
            athena_partitions_limit=100,
        )
        assert len(result) == 1
        assert result[0] == " or ".join(partitions)

    def test_partitions_exceeding_limit(self):
        partitions = [f"date_col='2024-01-{i:02d}'" for i in range(1, 6)]
        result = _render_batches(
            partitions=partitions,
            is_bucketed=False,
            bucket_conditions={},
            bucket_numbers=[],
            bucket_column=None,
            athena_partitions_limit=2,
        )
        assert len(result) == 3
        assert result[0] == "date_col='2024-01-01' or date_col='2024-01-02'"
        assert result[1] == "date_col='2024-01-03' or date_col='2024-01-04'"
        assert result[2] == "date_col='2024-01-05'"


class TestBucketOnlyBatching:
    """Tests for bucket partitioning without non-bucket partition columns."""

    def test_bucket_only_single_bucket(self):
        result = _render_batches(
            partitions=[],
            is_bucketed=True,
            bucket_conditions={0: ["'val1'", "'val2'", "'val3'"]},
            bucket_numbers=[0],
            bucket_column="user_id",
            athena_partitions_limit=100,
        )
        assert len(result) == 1
        assert result[0] == "user_id IN ('val1', 'val2', 'val3')"

    def test_bucket_only_with_empty_partition_string(self):
        """Bucket-only case where ns.partitions contains an empty string (real-world behavior).

        When all partition columns are bucket columns, single_partition is empty
        and single_partition_expression becomes ''. This must be filtered out
        to avoid generating WHERE () AND col IN (...).
        """
        result = _render_batches(
            partitions=[""],
            is_bucketed=True,
            bucket_conditions={0: ["'val1'", "'val2'"]},
            bucket_numbers=[0],
            bucket_column="user_id",
            athena_partitions_limit=100,
        )
        assert len(result) == 1
        assert result[0] == "user_id IN ('val1', 'val2')"

    def test_bucket_only_multiple_buckets(self):
        result = _render_batches(
            partitions=[],
            is_bucketed=True,
            bucket_conditions={
                0: ["'a'", "'b'"],
                1: ["'c'"],
                2: ["'d'", "'e'", "'f'"],
            },
            bucket_numbers=[0, 1, 2],
            bucket_column="col",
            athena_partitions_limit=100,
        )
        assert len(result) == 3
        assert result[0] == "col IN ('a', 'b')"
        assert result[1] == "col IN ('c')"
        assert result[2] == "col IN ('d', 'e', 'f')"


class TestBucketWithPartitionsBatching:
    """Tests for bucket + non-bucket partition columns (AND structure)."""

    def test_single_partition_single_bucket(self):
        result = _render_batches(
            partitions=["date_col='2024-01-01'"],
            is_bucketed=True,
            bucket_conditions={0: ["'val1'", "'val2'"]},
            bucket_numbers=[0],
            bucket_column="user_id",
            athena_partitions_limit=100,
        )
        assert len(result) == 1
        assert result[0] == "(date_col='2024-01-01') and user_id IN ('val1', 'val2')"

    def test_multiple_partitions_multiple_buckets(self):
        partitions = [f"date_col='2024-01-{i:02d}'" for i in range(1, 4)]
        result = _render_batches(
            partitions=partitions,
            is_bucketed=True,
            bucket_conditions={
                0: ["'a'"],
                1: ["'b'"],
            },
            bucket_numbers=[0, 1],
            bucket_column="col",
            athena_partitions_limit=100,
        )
        # 1 partition_batch (3 partitions < 100 limit) × 2 buckets = 2 batches
        assert len(result) == 2
        expected_pb = " or ".join(partitions)
        assert result[0] == f"({expected_pb}) and col IN ('a')"
        assert result[1] == f"({expected_pb}) and col IN ('b')"

    def test_partitions_split_into_batches_with_buckets(self):
        """Partitions exceeding the limit should be split, each combined with each bucket."""
        partitions = [f"d='{i}'" for i in range(5)]
        result = _render_batches(
            partitions=partitions,
            is_bucketed=True,
            bucket_conditions={
                0: ["'x'"],
                1: ["'y'"],
            },
            bucket_numbers=[0, 1],
            bucket_column="c",
            athena_partitions_limit=2,
        )
        # 3 partition_batches (ceil(5/2)) × 2 buckets = 6 batches
        assert len(result) == 6
        assert result[0] == "(d='0' or d='1') and c IN ('x')"
        assert result[1] == "(d='2' or d='3') and c IN ('x')"
        assert result[2] == "(d='4') and c IN ('x')"
        assert result[3] == "(d='0' or d='1') and c IN ('y')"
        assert result[4] == "(d='2' or d='3') and c IN ('y')"
        assert result[5] == "(d='4') and c IN ('y')"


class TestINClauseChunking:
    """Tests for IN clause chunking when values exceed max_in_bytes."""

    def test_no_chunking_when_under_limit(self):
        """Small IN clause should not be split."""
        values = ["'v1'", "'v2'", "'v3'"]
        result = _render_batches(
            partitions=["d='1'"],
            is_bucketed=True,
            bucket_conditions={0: values},
            bucket_numbers=[0],
            bucket_column="c",
            athena_partitions_limit=100,
            max_in_bytes_override=1000,
        )
        assert len(result) == 1
        assert result[0] == "(d='1') and c IN ('v1', 'v2', 'v3')"

    def test_chunking_splits_large_in_clause(self):
        """IN clause exceeding max_in_bytes should be split into chunks."""
        # 10 values of 5 chars each, joined with ", " → 68 chars total.
        # max_in_bytes=30 → ceil(68/30)=3 chunks, chunk_size=ceil(10/3)=4.
        values = [f"'{i:03d}'" for i in range(10)]
        result = _render_batches(
            partitions=["d='1'"],
            is_bucketed=True,
            bucket_conditions={0: values},
            bucket_numbers=[0],
            bucket_column="c",
            athena_partitions_limit=100,
            max_in_bytes_override=30,
        )
        assert len(result) == 3
        assert result[0] == "(d='1') and c IN ('000', '001', '002', '003')"
        assert result[1] == "(d='1') and c IN ('004', '005', '006', '007')"
        assert result[2] == "(d='1') and c IN ('008', '009')"

    def test_chunking_with_multiple_partition_batches(self):
        """Chunked IN clause combined with multiple partition batches."""
        partitions = [f"d='{i}'" for i in range(4)]
        # 10 values × 5 chars + 9 separators × 2 chars = 68 chars.
        # max 30 → 3 chunks, chunk_size=4.
        values = [f"'{i:03d}'" for i in range(10)]
        result = _render_batches(
            partitions=partitions,
            is_bucketed=True,
            bucket_conditions={0: values},
            bucket_numbers=[0],
            bucket_column="c",
            athena_partitions_limit=2,
            max_in_bytes_override=30,
        )
        # 2 partition_batches (ceil(4/2)) × 1 bucket × 3 chunks = 6 batches
        assert len(result) == 6
        # chunk 0 × partition_batch 0, 1
        assert result[0] == "(d='0' or d='1') and c IN ('000', '001', '002', '003')"
        assert result[1] == "(d='2' or d='3') and c IN ('000', '001', '002', '003')"
        # chunk 1 × partition_batch 0, 1
        assert result[2] == "(d='0' or d='1') and c IN ('004', '005', '006', '007')"
        assert result[3] == "(d='2' or d='3') and c IN ('004', '005', '006', '007')"
        # chunk 2 × partition_batch 0, 1
        assert result[4] == "(d='0' or d='1') and c IN ('008', '009')"
        assert result[5] == "(d='2' or d='3') and c IN ('008', '009')"

    def test_chunking_with_multiple_buckets(self):
        """Multiple buckets each with different chunk counts."""
        # bucket 0: 6 values of 3 chars, joined = "'a', 'b', 'c', 'd', 'e', 'f'" = 29 chars
        #   max 20 → ceil(29/20)=2 chunks, chunk_size=ceil(6/2)=3
        # bucket 1: 2 values, joined = "'x', 'y'" = 8 chars → 1 chunk
        result = _render_batches(
            partitions=["d='1'"],
            is_bucketed=True,
            bucket_conditions={
                0: ["'a'", "'b'", "'c'", "'d'", "'e'", "'f'"],
                1: ["'x'", "'y'"],
            },
            bucket_numbers=[0, 1],
            bucket_column="col",
            athena_partitions_limit=100,
            max_in_bytes_override=20,
        )
        # bucket 0: 2 chunks × 1 partition_batch = 2
        # bucket 1: 1 chunk × 1 partition_batch = 1
        assert len(result) == 3
        assert result[0] == "(d='1') and col IN ('a', 'b', 'c')"
        assert result[1] == "(d='1') and col IN ('d', 'e', 'f')"
        assert result[2] == "(d='1') and col IN ('x', 'y')"

    def test_bucket_only_with_chunking(self):
        """Bucket-only case (no partitions) with IN clause chunking."""
        # 10 values × 5 chars + 9 × 2 separators = 68 chars. max 30 → 3 chunks of 4.
        values = [f"'{i:03d}'" for i in range(10)]
        result = _render_batches(
            partitions=[],
            is_bucketed=True,
            bucket_conditions={0: values},
            bucket_numbers=[0],
            bucket_column="c",
            athena_partitions_limit=100,
            max_in_bytes_override=30,
        )
        assert len(result) == 3
        assert result[0] == "c IN ('000', '001', '002', '003')"
        assert result[1] == "c IN ('004', '005', '006', '007')"
        assert result[2] == "c IN ('008', '009')"


class TestRealisticScenario:
    """Test with parameters closer to a real-world scenario."""

    def test_many_partitions_many_buckets(self):
        """365 dates × 3 buckets, partition limit 100."""
        partitions = [f"date_col=DATE'{2024}-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}'" for i in range(365)]
        bucket_conditions = {
            0: [f"'{i}'" for i in range(100)],
            1: [f"'{i}'" for i in range(100, 200)],
            2: [f"'{i}'" for i in range(200, 250)],
        }
        result = _render_batches(
            partitions=partitions,
            is_bucketed=True,
            bucket_conditions=bucket_conditions,
            bucket_numbers=[0, 1, 2],
            bucket_column="user_id",
            athena_partitions_limit=100,
        )
        # partition_batches: ceil(365/100) = 4
        # 3 buckets × 1 chunk each × 4 partition_batches = 12
        assert len(result) == 12

        # Verify structure: each batch has AND structure
        for batch in result:
            assert ") and user_id IN (" in batch

        # Verify no IN clause duplication within a single batch
        for batch in result:
            assert batch.count("IN (") == 1
