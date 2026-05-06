# Implementation Plan: S3 Table Bucket Support for dbt-athena

## Architecture Decisions

### Detection Mechanism

S3 Table Buckets are accessed through named Athena data catalogs registered with `--type GLUE --parameters catalog-id=<account>:s3tablescatalog/<bucket>`. The `GetDataCatalog` API returns:

```json
{
  "DataCatalog": {
    "Name": "my_s3_tables",
    "Type": "GLUE",
    "Parameters": {"catalog-id": "182399687476:s3tablescatalog/dbt-athena-test"}
  }
}
```

Detection: check if `Parameters["catalog-id"]` contains the substring `s3tablescatalog/`. No new catalog type enum needed — it's a GLUE-type catalog from Athena's perspective.

A new `@available` method `is_s3_table_bucket(database: str) -> bool` on `AthenaAdapter` wraps this check. It's cached via `@lru_cache` since `_get_data_catalog()` currently makes an uncached API call on every invocation (called 14 times across the adapter). Macros access it as `adapter.is_s3_table_bucket(relation.database)`.

### Why Most Glue API Calls Work As-Is

The existing `get_catalog_id()` helper extracts `Parameters["catalog-id"]` verbatim for GLUE-type catalogs. Since S3TB catalogs register as type GLUE, `get_catalog_id()` returns the compound ID (e.g., `182399687476:s3tablescatalog/dbt-athena-test`). AWS Glue APIs accept compound `CatalogId` for federated catalogs. This means the following methods work without modification:

- `get_glue_table()` — passes CatalogId to `glue.get_table()`
- `get_columns_in_relation()` — passes CatalogId conditionally (truthy check)
- `list_relations_without_caching()` — passes CatalogId conditionally
- `delete_from_glue_catalog()` — passes CatalogId to `glue.delete_table()`
- `persist_docs_to_glue()` — passes CatalogId to `glue.get_table()` + `glue.update_table()`
- `check_schema_exists()` — passes CatalogId to `glue.get_database()`
- `clean_up_partitions()` — passes CatalogId (irrelevant for S3TB since Iceberg has no Glue partitions, but harmless)

### Drop Path

No `s3tables` boto3 client needed. The existing `delete_from_glue_catalog()` calls `glue.delete_table(CatalogId=compound_id, ...)` which handles S3TB tables. The change is: skip `clean_up_table()` (the S3 object deletion step) for S3TB targets. The macro `drop_relation_glue` becomes: skip `adapter.clean_up_table()` → call `adapter.delete_from_glue_catalog()` only.

For `native_drop=true` (SQL `DROP TABLE`): blocked by AWS on S3TB. The adapter forces the Glue API path and logs a warning.

### Table Materialization: Drop-and-Recreate

Since `ALTER TABLE RENAME` is blocked by AWS on S3TB, the rename-swap pattern used for zero-downtime Iceberg deployments is impossible. The table materialization uses drop-and-recreate instead: drop existing table via Glue API, then CTAS directly to the target relation. Brief downtime is accepted (documented as known limitation).

### CTAS DDL

S3TB CTAS omits `table_type`, `is_external`, and `location` from the WITH clause:

```sql
-- Regular Iceberg (current)
CREATE TABLE schema.model WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://bucket/path/',
  format = 'PARQUET',
  partitioning = ARRAY['day(event_date)']
) AS SELECT ...

-- S3 Table Bucket (new)
CREATE TABLE schema.model WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(event_date)']
) AS SELECT ...
```

### Schema Creation

The existing `CREATE SCHEMA IF NOT EXISTS` SQL macro uses `render_hive()` which strips the database name. Since pyathena's connection has `catalog_name=creds.database` set to the S3TB catalog name, the DDL targets the correct catalog. If SQL schema creation doesn't work for S3TB (open question), a fallback to `glue.create_database(CatalogId=compound_id, ...)` will be added.

---

## Key Components and Responsibilities

### Python Layer (`src/dbt/adapters/athena/`)

**`impl.py`**:
- Add `is_s3_table_bucket(database: str) -> bool` — cached detection via `_get_data_catalog()`, checks `catalog-id` for `s3tablescatalog/` substring
- Add `@lru_cache` to `_get_data_catalog()` — currently makes uncached API calls, called 14+ times per run
- Modify `clean_up_table()` — skip S3 deletion for S3TB targets (or let macros gate this)
- Modify `list_schemas()` — pass `CatalogId` from `_get_data_catalog()` (existing bug: never passes CatalogId)
- Modify `generate_s3_location()` — return `None` or skip for S3TB (macros may gate this instead)

**`utils.py`**:
- No enum changes needed — S3TB catalogs register as `Type: "GLUE"`
- `get_catalog_id()` works as-is for S3TB (returns compound catalog-id string)

**`connections.py`**:
- No changes needed — pyathena connection works with any catalog name

**`relation.py`**:
- No changes needed — `get_table_type()` should correctly detect Iceberg tables from S3TB since Glue returns table metadata in the same format

### Macro Layer (`src/dbt/include/athena/macros/`)

**`materializations/models/table/create_table_as.sql`**:
- Set `is_s3tb = adapter.is_s3_table_bucket(relation.database)` at top
- When `is_s3tb`: skip `adapter.generate_s3_location()`, skip `adapter.delete_from_s3()`, omit `table_type`/`is_external`/`location` from WITH clause
- When `is_s3tb`: skip the unique-location validation for Iceberg (irrelevant since no RENAME)
- When `is_s3tb` and `language == 'python'`: raise error
- Default `table_type` to `'iceberg'` when `is_s3tb` and not explicitly set to `'iceberg'` (error if set to `'hive'`)

**`materializations/models/table/table.sql`**:
- When `is_s3tb`: use drop-and-recreate instead of rename-swap
- Flow: `delete_from_glue_catalog(old_relation)` → `safe_create_table_as(target_relation, ...)`
- When `is_s3tb` and `language == 'python'`: raise error

**`materializations/models/incremental/incremental.sql`**:
- When `is_s3tb`: default `table_type` to `'iceberg'`
- When `is_s3tb` and `should_full_refresh()`: use drop-and-recreate instead of rename-swap
- When `is_s3tb` and `language == 'python'`: raise error
- Normal incremental paths (merge, append) work unchanged

**`adapters/relation.sql`**:
- `athena__drop_relation`: for S3TB, force Glue API path (skip `clean_up_table`, call `delete_from_glue_catalog` only). Warn if `native_drop=true`.
- `athena__rename_relation`: add defensive guard — raise error if called for S3TB relation

**`materializations/seeds/helpers.sql`**:
- Add guard at top of `athena__create_csv_table`: raise error for S3TB targets

**`materializations/models/view/view.sql`** (or wherever the view materialization entry point is):
- Add guard: raise error for S3TB targets

**`materializations/snapshots/snapshot.sql`**:
- When `is_s3tb`: default `table_type` to `'iceberg'`, raise error if `table_type='hive'`
- Iceberg snapshot path (MERGE INTO) works unchanged
- Initial creation flows through fixed `create_table_as`

---

## Integration Points and External Dependencies

### AWS APIs Used

| API | Service | Purpose | S3TB Behavior |
|-----|---------|---------|---------------|
| `GetDataCatalog` | Athena | Detect catalog type | Returns GLUE type with compound catalog-id |
| `GetTable` | Glue | Read table metadata | Works with compound CatalogId |
| `GetTables` | Glue | List relations | Works with compound CatalogId |
| `DeleteTable` | Glue | Drop tables | Works with compound CatalogId (replaces S3 cleanup + SQL DROP) |
| `GetDatabase` | Glue | Check schema exists | Works with compound CatalogId |
| `GetDatabases` | Glue | List schemas | Needs CatalogId fix (currently missing) |
| `CreateDatabase` | Glue | Create namespace | Needs verification (research item) |
| `DeleteObjects` | S3 | Clean up table data | **Skipped** for S3TB |
| `ListObjectsV2` | S3 | Check S3 path exists | **Skipped** for S3TB |

### pyathena

No changes needed. pyathena passes `catalog_name` to Athena, which routes queries to the correct catalog.

### boto3

No new service clients needed. Existing `glue` and `athena` clients suffice.

---

## Implementation Strategy

### Work Items (ordered by dependency)

#### WI-1: Detection Mechanism
**Files**: `impl.py`
**Changes**:
- Add `@lru_cache` to `_get_data_catalog()`
- Add `@available` method `is_s3_table_bucket(database: str) -> bool`
- Detection logic: get catalog via `_get_data_catalog()`, check `Type == "GLUE"` and `"s3tablescatalog/"` in `Parameters.get("catalog-id", "")`
**Tests**: Unit test with mocked `_get_data_catalog` return values
**Depends on**: nothing

#### WI-2: CTAS DDL Changes
**Files**: `create_table_as.sql`
**Changes**:
- Set `is_s3tb` flag from `adapter.is_s3_table_bucket(relation.database)`
- When `is_s3tb`: default `table_type` to `'iceberg'`, error if `'hive'`
- When `is_s3tb`: skip `generate_s3_location()` and `delete_from_s3()`
- When `is_s3tb`: omit `table_type`, `is_external`, `location` from WITH clause
- When `is_s3tb`: skip unique-location validation
- When `is_s3tb` and Python: raise error
**Tests**: Unit test via Jinja2 template rendering (pattern from `test_get_partition_batches.py`)
**Depends on**: WI-1

#### WI-3: Drop Path Changes
**Files**: `relation.sql`, `impl.py`
**Changes**:
- `athena__drop_relation`: for S3TB, skip `clean_up_table()`, call `delete_from_glue_catalog()` directly. Log warning if `native_drop=true`.
- `athena__rename_relation`: add defensive error guard for S3TB
**Tests**: Unit test for drop routing logic
**Depends on**: WI-1

#### WI-4: Table Materialization
**Files**: `table.sql`
**Changes**:
- When `is_s3tb`: replace rename-swap with drop-and-recreate
- Flow: if old_relation exists → `delete_from_glue_catalog(old_relation)` → `safe_create_table_as(target_relation, ...)`
- When `is_s3tb` and Python: raise error
**Tests**: Unit test for materialization routing
**Depends on**: WI-1, WI-2, WI-3

#### WI-5: Incremental Materialization
**Files**: `incremental.sql`
**Changes**:
- When `is_s3tb`: default `table_type` to `'iceberg'`
- When `is_s3tb` and `should_full_refresh()`: drop-and-recreate instead of rename-swap
- When `is_s3tb` and Python: raise error
- Normal merge/append paths: unchanged
**Tests**: Unit test for full-refresh routing
**Depends on**: WI-1, WI-2

#### WI-6: Snapshot Materialization
**Files**: `snapshot.sql`
**Changes**:
- When `is_s3tb`: default `table_type` to `'iceberg'`, error if `'hive'`
- Iceberg snapshot MERGE path: unchanged
- Initial creation: flows through fixed `create_table_as`
**Tests**: Minimal — inherits WI-2 CTAS fixes
**Depends on**: WI-1, WI-2

#### WI-7: Error Guards
**Files**: `seeds/helpers.sql`, view materialization macro, `create_table_as.sql` (Python guard)
**Changes**:
- Seeds: raise `CompilationError` at top of `athena__create_csv_table`
- Views: raise `CompilationError` at top of view materialization
- Python models: raise `CompilationError` in `create_table_as.sql` and materialization entry points
**Tests**: Unit test that errors are raised
**Depends on**: WI-1

#### WI-8: list_schemas Fix
**Files**: `impl.py`
**Changes**:
- Pass `CatalogId` from `_get_data_catalog(database)` to `glue.get_databases()` paginator
- Follows same pattern as `check_schema_exists()` which already does this correctly
**Tests**: Unit test with mock Glue client
**Depends on**: WI-1

#### WI-9: Unit Tests
**Files**: `tests/unit/test_adapter.py`, potentially new `tests/unit/test_s3_table_bucket.py`
**Changes**:
- Test `is_s3_table_bucket()` with various catalog responses (S3TB, regular GLUE, LAMBDA, HIVE, None)
- Test `list_schemas()` passes CatalogId
- Test DDL generation via Jinja2 template rendering for S3TB (no location, no table_type, no is_external)
- Test drop routing for S3TB (skip clean_up_table, call delete_from_glue_catalog)
- Test error guards (seeds, views, Python models)
- Use `botocore.client.BaseClient._make_api_call` patching pattern (from existing `test__get_one_catalog_federated_query_catalog`) since moto doesn't support S3 Tables
**Depends on**: WI-1 through WI-8

#### WI-10: Integration Tests
**Files**: `tests/functional/` (new test files)
**Changes**:
- Test against real S3 Table Bucket: `arn:aws:s3tables:eu-north-1:182399687476:bucket/dbt-athena-test`
- Test table materialization (create, re-run/replace)
- Test incremental merge
- Test incremental append
- Test full-refresh
- Test snapshot
- Verify seed/view/Python errors
**Depends on**: WI-1 through WI-8, requires AWS credentials

### Parallelization

WI-1 is the foundation. After WI-1:
- WI-2, WI-3, WI-7, WI-8 can be done in parallel
- WI-4, WI-5, WI-6 depend on WI-2 and WI-3
- WI-9 and WI-10 come last

```
WI-1 (detection)
 ├── WI-2 (CTAS DDL)
 │    ├── WI-4 (table mat)
 │    ├── WI-5 (incremental mat)
 │    └── WI-6 (snapshot mat)
 ├── WI-3 (drop path)
 │    └── WI-4 (table mat)
 ├── WI-7 (error guards)
 └── WI-8 (list_schemas)
      └── WI-9 (unit tests)
           └── WI-10 (integration tests)
```

### Risk Mitigation

- **Schema creation uncertainty**: If `CREATE SCHEMA` via SQL doesn't work for S3TB, fall back to `glue.create_database()` with compound CatalogId in the Python layer. This is a small, isolated change.
- **Glue `delete_table` for S3TB**: If `glue.delete_table(CatalogId=compound_id)` doesn't delete S3TB tables, escalate to using `s3tables.delete_table()` — requires adding `boto3.client('s3tables')` and a new dependency on `boto3-stubs[s3tables]`. Keep this as a fallback, not the default plan.
- **`get_table_type()` for S3TB tables**: If Glue returns different metadata for S3TB tables than regular Iceberg tables, `get_table_type()` may need a fix. Verify during WI-10 integration testing.
