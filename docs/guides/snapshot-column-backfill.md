# Snapshot Column Backfill

## Overview

When a new column is added to a snapshot source, dbt automatically adds the column to the snapshot table via `ALTER TABLE ADD COLUMN`. By default, historical rows remain with NULL values for the new column.

The **Snapshot Column Backfill** feature provides an optional mechanism to automatically backfill historical snapshot rows with current source values when new columns are detected.

## ⚠️ Critical Limitation

**WARNING**: Backfilled data represents **CURRENT source values**, NOT what the values were at each historical snapshot time. This is fundamentally unavoidable - the historical data simply doesn't exist.

| dbt_valid_from | status | tier (backfilled) | What Actually Happened |
|----------------|--------|-------------------|------------------------|
| 2024-01-01 | pending | "premium" | tier column didn't exist |
| 2024-02-01 | shipped | "premium" | tier was actually "basic" |
| 2024-03-01 | delivered | "premium" | tier is currently "premium" |

Users must explicitly opt-in to this behavior by enabling both the behavior flag AND the snapshot config.

## Enabling the Feature

### Step 1: Enable the Behavior Flag

The feature is gated behind a behavior flag for safety. Enable it in your `dbt_project.yml`:

```yaml
vars:
  dbt_snapshot_backfill_enabled: true
```

### Step 2: Configure the Snapshot

Add the backfill configuration to your snapshot:

```yaml
# snapshots/orders_snapshot.yml
snapshots:
  - name: orders_snapshot
    config:
      backfill_new_columns: true
      backfill_audit_column: dbt_backfill_audit  # optional
```

Or in the snapshot SQL:

```sql
{% snapshot orders_snapshot %}
    {{ config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='id',
        updated_at='updated_at',
        backfill_new_columns=true,
        backfill_audit_column='dbt_backfill_audit',
    ) }}
    
    select * from {{ source('orders', 'orders') }}
{% endsnapshot %}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `backfill_new_columns` | bool/string | `false` | Enable backfill. Values: `true`/`'source'` (backfill from source), `false`/`'null'` (keep NULL) |
| `backfill_audit_column` | string | `null` | Column name to store JSON audit dictionary tracking per-column backfill timestamps |

## Audit Column

When `backfill_audit_column` is configured, dbt creates a TEXT column containing a JSON dictionary that tracks when each column was backfilled:

```json
{"tier": "2024-01-30T10:00:00Z", "region": "2024-02-28T10:00:00Z"}
```

### Multi-Column Backfill Over Time

The audit column accumulates entries as columns are added:

| Run | New Columns | `dbt_backfill_audit` |
|-----|-------------|----------------------|
| Day 30 | tier | `{"tier": "2024-01-30T10:00:00Z"}` |
| Day 60 | region | `{"tier": "2024-01-30T10:00:00Z", "region": "2024-02-28T10:00:00Z"}` |
| Day 90 | score, rank | `{"tier": "...", "region": "...", "score": "...", "rank": "..."}` |

### Querying Audit Data

```sql
-- Find rows where 'tier' was backfilled (Postgres/Snowflake)
SELECT * FROM snapshot_table 
WHERE dbt_backfill_audit::json->>'tier' IS NOT NULL;

-- Simple LIKE query for any database
SELECT * FROM snapshot_table 
WHERE dbt_backfill_audit LIKE '%"tier"%';
```

## NULL Value Handling

When a source column value is NULL, the snapshot row value stays NULL, but the audit column is still updated. This allows you to distinguish between:

| Scenario | Column Value | Audit JSON | Interpretation |
|----------|--------------|------------|----------------|
| Column didn't exist when snapshot taken | NULL | NULL | Never backfilled |
| Source value is actually NULL | NULL | `{"col": "2024-..."}` | Backfilled, source was NULL |
| Source has a value | "value" | `{"col": "2024-..."}` | Backfilled with value |

## Adapter Support

| Adapter | SQL Syntax | Notes |
|---------|------------|-------|
| **Postgres** | UPDATE...FROM | Full support |
| **Snowflake** | UPDATE...FROM | Full support |
| **Redshift** | UPDATE...FROM | Full support |
| **BigQuery** | MERGE | Full support |
| **Spark/Databricks** | MERGE | Requires Delta/Iceberg/Hudi format |
| **Athena** | MERGE | Requires Iceberg tables only |

## Alternative: Downstream Join

If you need strict historical accuracy, consider a downstream model instead:

```sql
-- downstream model that joins current values without modifying snapshot
SELECT 
    s.*,
    COALESCE(s.new_col, current.new_col) as new_col_filled
FROM {{ ref('snapshot_table') }} s
LEFT JOIN {{ ref('current_source') }} current 
    ON s.unique_key = current.unique_key
```

## Performance Considerations

- **Large tables**: UPDATE on millions of rows is expensive
- **Recommendation**: Schedule snapshots during off-peak hours
- **Future enhancement**: Batching support for very large tables

## Example Warning Log

When backfill runs, dbt emits a warning:

```
WARNING: Backfilling 2 new column(s) [tier, region] in snapshot 'orders_snapshot'. 
         Historical rows will be populated with CURRENT source values, not 
         point-in-time historical values.
```
