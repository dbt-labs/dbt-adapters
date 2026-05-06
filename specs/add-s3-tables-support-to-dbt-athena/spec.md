# Add S3 Table Bucket Support to dbt-athena

## Problem Statement

AWS S3 Table Buckets (launched Dec 2024) provide fully managed Apache Iceberg tables with automatic compaction, snapshot management, and garbage collection. Athena fully supports querying and writing to S3 Table Buckets via federated Glue catalogs.

dbt-athena cannot currently target S3 Table Bucket tables because:

1. **Location is forbidden**: S3 Table Buckets manage their own storage. dbt-athena always generates an S3 location and emits it in CREATE TABLE DDL — Athena rejects this.
2. **WITH clause properties differ**: S3 Table Bucket CTAS must omit `table_type`, `is_external`, and `location` from the WITH clause. Passing any of these causes errors (e.g. "Table type customer is not supported").
3. **S3 cleanup operations fail**: The adapter calls `s3:DeleteObjects` and `s3:ListObjectsV2` before table creation and on drop. These standard S3 APIs don't work against managed Table Bucket storage.
4. **ALTER TABLE RENAME is not supported**: AWS blocks RENAME on S3 Table Bucket tables, breaking the zero-downtime swap used by table and incremental materializations.
5. **DROP TABLE via SQL is not supported**: AWS blocks `DROP TABLE` SQL statements on S3 Table Bucket tables; deletion must go through the Glue API or S3 Tables API.

A community member confirmed these blockers in [issue #1186](https://github.com/dbt-labs/dbt-adapters/issues/1186) — they got past the catalog detection issue using a named Athena data catalog, but were blocked at the CTAS location/table_type level.

## User-Facing Requirements

### 1. Profile Configuration

Users configure their dbt profile to target an S3 Table Bucket by setting `database` to a **named Athena data catalog** that points to the S3 Table Bucket's federated Glue catalog.

**Prerequisite** (user performs once, outside dbt):
```bash
aws athena create-data-catalog \
  --name my_s3_tables \
  --type GLUE \
  --parameters catalog-id=182399687476:s3tablescatalog/my-bucket
```

**Profile** (`profiles.yml`):
```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: athena
      database: my_s3_tables          # the named Athena data catalog
      schema: my_namespace            # S3 Table Bucket namespace
      s3_staging_dir: s3://my-staging-bucket/athena-results/
      region_name: eu-north-1
      # s3_data_dir is NOT required — storage is managed
```

The adapter auto-detects that this catalog targets an S3 Table Bucket (by inspecting the `catalog-id` parameter from `GetDataCatalog` for the `s3tablescatalog/` pattern) and adjusts its behavior accordingly.

### 2. Table Materialization

Users can materialize models as tables into S3 Table Buckets.

```sql
{{ config(
    materialized='table',
    table_type='iceberg',
    format='parquet',
    partitioning=["day(event_date)"]
) }}

SELECT * FROM source_data
```

**Behavior**:
- The adapter emits CTAS without `location`, `table_type`, or `is_external` in the WITH clause.
- No S3 cleanup operations are performed before or after table creation.
- On re-run, the existing table is dropped (via Glue API) and recreated. There is a brief period of downtime — zero-downtime swap is not possible because AWS does not support `ALTER TABLE RENAME` on S3 Table Bucket tables.

### 3. Incremental Materialization

Users can use incremental models with the `merge` and `append` strategies.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    table_type='iceberg',
    format='parquet'
) }}

SELECT * FROM source_data
{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
```

**Behavior**:
- `merge`: Uses standard Iceberg `MERGE INTO` SQL — works unchanged.
- `append`: Uses standard `INSERT INTO` SQL — works unchanged.
- `insert_overwrite`: Not applicable (Iceberg-only, uses `merge` or `append`).
- `--full-refresh`: Drops and recreates the table (same downtime caveat as table materialization).

### 4. Snapshot Materialization

Users can create snapshots targeting S3 Table Buckets.

**Behavior**:
- Initial snapshot creation uses the adapted CTAS (no location).
- Ongoing snapshots use Iceberg `MERGE INTO` SQL — works unchanged.

### 5. Schema Operations

- `dbt run` creates namespaces (databases) in the S3 Table Bucket if they don't exist, via the Glue API with the compound `CatalogId`.
- `dbt run` lists existing tables/relations in the namespace via the Glue API.
- Schema change handling (`on_schema_change`) uses standard Iceberg DDL (`ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`) — works unchanged.

### 6. Graceful Errors for Unsupported Operations

The following operations raise clear, user-friendly errors when targeting an S3 Table Bucket:

| Operation | Error Message |
|---|---|
| **Seeds** | "Seeds are not supported for S3 Table Bucket targets. Load seed data through an alternative method (e.g. INSERT INTO from a staging table in a regular catalog)." |
| **Views** | "CREATE VIEW is not supported on S3 Table Bucket catalogs by AWS." |
| **Python/Spark models** | "Python models targeting S3 Table Buckets are not yet supported." |

## Success Criteria

1. A user can configure a dbt profile pointing to a named Athena data catalog backed by an S3 Table Bucket and successfully run `dbt run` with table, incremental (merge/append), and snapshot materializations.
2. The adapter auto-detects S3 Table Bucket catalogs without requiring any new profile-level or model-level flags.
3. CTAS statements omit `location`, `table_type`, and `is_external` when targeting S3 Table Buckets.
4. No S3 object-level API calls (`DeleteObjects`, `ListObjectsV2`, etc.) are made against S3 Table Bucket managed storage.
5. Table drops use the Glue API (`delete_table` with compound `CatalogId`) instead of SQL `DROP TABLE` or S3 cleanup.
6. Seeds, views, and Python models raise clear errors when targeting S3 Table Buckets.
7. All existing dbt-athena unit tests continue to pass (no regressions for regular Athena usage).
8. New unit tests cover S3 Table Bucket detection logic, DDL generation (no location), and drop behavior.
9. Integration tests pass against a real S3 Table Bucket (`arn:aws:s3tables:eu-north-1:182399687476:bucket/dbt-athena-test`).

## Out of Scope

- **Spark/Python models**: Blocked; documented as known limitation with clear error.
- **Seeds**: Blocked; documented as known limitation with clear error.
- **Views**: Blocked by AWS; documented with clear error.
- **Zero-downtime table swap**: Blocked by AWS (`ALTER TABLE RENAME` not supported on S3 Table Buckets). Drop-and-recreate is the only option.
- **Direct `s3tablescatalog/<bucket>` as `database`**: Users must register a named Athena data catalog. This is the AWS-recommended approach.
- **S3 Tables API client (`boto3.client('s3tables')`)**: Not needed in this phase — Glue API with compound `CatalogId` handles catalog operations.
- **Hive table type on S3 Table Buckets**: Not applicable — S3 Table Buckets only support Iceberg.
- **`insert_overwrite` incremental strategy**: This is a Hive-only strategy; S3 Table Bucket users should use `merge` or `append`.
- **Lake Formation tag management on S3 Table Bucket catalogs**: Untested, out of scope.

## Design Decisions

1. **`table_type` auto-defaults to `'iceberg'`**: When targeting an S3 Table Bucket, the adapter defaults `table_type` to `'iceberg'` if not explicitly set. S3 Table Buckets only support Iceberg — requiring users to specify it in every model config is unnecessary boilerplate. If a user explicitly sets `table_type='iceberg'`, it is accepted silently. If they set `table_type='hive'`, it raises an error.
2. **`native_drop` override with warning**: SQL `DROP TABLE` is blocked by AWS on S3 Table Buckets. The adapter always uses Glue API deletion for S3TB targets regardless of the `native_drop` setting. If a user explicitly sets `native_drop=true`, a warning is logged explaining that SQL DROP is not supported on S3 Table Buckets and Glue API deletion is used instead.

## Resolved Questions

1. **Namespace (database) creation**: ✅ Verified. Both `glue.create_database(CatalogId=compound_id)` and Athena SQL `CREATE SCHEMA IF NOT EXISTS` (with catalog context) work for S3 Table Buckets. No fallback needed.
