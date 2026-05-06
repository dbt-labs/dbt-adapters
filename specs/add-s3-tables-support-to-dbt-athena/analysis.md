# Analysis: S3 Table Bucket Support — Pre-Implementation Quality Gate

## Risk Assessment

### HIGH: Incremental merge temp tables live in the S3TB catalog

The `incremental.sql` merge path creates a temp table (`__dbt_tmp`) that inherits the target relation's catalog/database/schema. For S3TB, this means the temp table is created as a managed S3TB Iceberg table (via the fixed CTAS — no location). It's used in `MERGE INTO`, then dropped via Glue API.

**This should work** — there's no restriction on table naming in S3TB, and the fixed `create_table_as` handles S3TB correctly. However:

- Each temp table creation triggers S3TB's managed storage allocation and background compaction setup — heavier than a regular S3-backed Hive temp table.
- If the dbt run fails mid-merge, the orphaned `__dbt_tmp` table persists in the S3TB catalog with no automatic cleanup (no S3 prefix to wipe). Subsequent runs attempt to drop it first, which should work via Glue API.
- **Not a blocker**, but worth noting in docs that incremental merge on S3TB has higher per-run overhead than regular Athena.

**Risk level**: Medium. Functionally correct, performance unknown until tested.

### HIGH: `table_type` default detection is blind

`config.get('table_type', default='hive')` returns `'hive'` whether the user explicitly set `table_type='hive'` or omitted it entirely. The spec says "error if user explicitly sets hive, silently default to iceberg otherwise" — but these cases are indistinguishable.

**Fix**: Use `config.get('table_type')` without a default first, then apply logic:
```jinja
{%- set raw_table_type = config.get('table_type') -%}
{%- if is_s3tb and raw_table_type is not none and raw_table_type | lower == 'hive' -%}
  {% do exceptions.raise_compiler_error("S3 Table Buckets only support Iceberg tables.") %}
{%- elif is_s3tb -%}
  {%- set table_type = 'iceberg' -%}
{%- else -%}
  {%- set table_type = (raw_table_type or 'hive') | lower -%}
{%- endif -%}
```

This pattern must be replicated in `create_table_as.sql`, `table.sql`, `incremental.sql`, and `snapshot.sql`. **Update tasks T-2, T-4, T-5, T-6 to use this pattern.**

### MEDIUM: `expire_glue_table_versions` fires on every incremental run

`incremental.sql` line 249 calls `adapter.expire_glue_table_versions(target_relation, 1, False)` unconditionally — including for Iceberg tables. The underlying `_get_glue_table_versions_to_expire` helper does NOT pass `CatalogId` to the Glue `get_table_versions` paginator. For S3TB:

- The call hits the default account catalog instead of the S3TB catalog
- Best case: returns empty (table doesn't exist in default catalog), no-op
- Worst case: finds a table with the same name in the default catalog and prunes its versions

**Fix options**:
1. Guard it: skip the call when `is_s3tb` (S3TB manages its own versioning)
2. Fix `_get_glue_table_versions_to_expire` to accept and pass `CatalogId`

Option 1 is simpler and correct — S3TB handles snapshot/version lifecycle automatically. **Add to T-5.**

### MEDIUM: `add_lf_tags_to_database` called after schema creation

`athena__create_schema` (schema.sql) calls `adapter.add_lf_tags_to_database(relation)` after every `CREATE SCHEMA`. For S3TB namespaces, Lake Formation tags may not be applicable and could error.

**Fix**: The existing `add_lf_tags_to_database` implementation (impl.py ~line 170) only fires if `lf_tags_database` credential is set. If users don't set LF tags in their profile (likely for S3TB), this is a no-op. **Low risk in practice, but add a defensive `is_s3_table_bucket` guard if `lf_tags_database` is set.** Can be deferred — document as known limitation for now.

### LOW: `external_location` not declared in `incremental.sql`

Line 72 of `incremental.sql` references `external_location` in a condition, but it's never declared via `config.get('external_location')`. This is a pre-existing latent bug — for S3TB it's harmless since the entire branch (rename-swap for full-refresh) is skipped. But worth knowing it exists.

### LOW: `force_batch=True` with S3TB

The `create_table_as_with_partitions` path (triggered by `force_batch=True`) creates a Hive-style unpartitioned staging table, then inserts in batches using `partitioned_by` syntax. This is incompatible with S3TB/Iceberg `partitioning` syntax. Risk is low because `force_batch` defaults to `False` and is a niche Hive optimization. **Add a guard in `create_table_as.sql`: error if `force_batch=True` and `is_s3tb`.**

### LOW: `DROP SCHEMA ... CASCADE` behavior on S3TB

Untested whether `DROP SCHEMA IF EXISTS namespace CASCADE` works on an S3TB catalog. If a user runs `dbt run-operation drop_schema`, this could fail. **Low risk** — schema drops are rare in normal dbt workflows. Document as untested.

---

## Gap Analysis

### 1. The `lru_cache` on `_get_data_catalog` is correct but `is_s3_table_bucket` should cache separately

`_get_data_catalog` is called by many methods for different purposes (get catalog_id, check type, etc.). Adding `@lru_cache` there is fine. But `is_s3_table_bucket` should be a thin wrapper that also caches — don't make it call `_get_data_catalog` every time and re-parse. Since `_get_data_catalog` is cached, the double-call is just a dict lookup, but it's cleaner to cache `is_s3_table_bucket` directly.

**Verdict**: Plan is fine as-is. `@lru_cache` on `_get_data_catalog` means `is_s3_table_bucket` is effectively cached too.

### 2. `_get_one_catalog` (dbt docs generate) not addressed

`_get_one_catalog` (impl.py ~line 620) generates the catalog for `dbt docs generate`. It branches on `AthenaCatalogType.GLUE` vs non-GLUE. For S3TB (which registers as GLUE), it takes the GLUE path and calls `glue.get_tables()` with `CatalogId`. This should work as-is since compound `CatalogId` is supported. **No gap — not mentioned in tasks but covered by existing Glue API pass-through.**

### 3. View guard location underspecified

T-6 says "find the view materialization entry point." The investigation confirms it's `materializations/models/view/view.sql` — a full Athena override exists. The guard should go at the top of `{% materialization view, adapter='athena' %}`, checking `adapter.is_s3_table_bucket(database)` where `database` is the template-scope variable. **Update T-6 with the exact file path.**

### 4. `table.sql` sub-case B not addressed: replacing a view with a table

When `old_relation.is_view` (table.sql ~line 103), the current iceberg path creates a temp table, drops the view, then renames temp → target. For S3TB, RENAME is blocked. This sub-case needs the same drop-and-recreate treatment as sub-case C. **Ensure T-4 covers this path.**

### 5. Snapshot `hive_snapshot_merge_sql` path creates tables

The hive snapshot update path calls `adapter.drop_relation(target_relation)` then `create_table_as(False, target_relation, sql)`. For S3TB, `table_type` would be forced to `'iceberg'`, so the hive snapshot path should be unreachable. But if it IS reached (e.g. user explicitly sets `table_type='hive'` before the guard fires), it would fail. **The T-6 guard on `table_type='hive'` must fire before the create/update branch, not inside it.**

### 6. No task covers updating `plan.md` account ID

The spec and plan still reference the old account ID `182399687476` in several places (spec.md success criteria #9, plan.md examples). **Minor — fix during T-10 finalization.**

---

## Edge Cases Not Covered

### 1. Multi-catalog dbt project

A user might have some models targeting regular Athena (AwsDataCatalog) and others targeting S3TB, using `database` overrides in model config:
```sql
{{ config(materialized='table', database='my_s3_tables') }}
```
The `is_s3_table_bucket` check uses `relation.database`, which correctly reflects per-model overrides. **This should work**, but no test covers it. **Add a test case to T-8 or T-9.**

### 2. `on_schema_change` with S3TB

When an incremental model's schema changes, `on_schema_change.sql` runs ALTER TABLE DDL. For S3TB, ALTER TABLE ADD/DROP/RENAME COLUMN should work (Iceberg DDL is supported). But `ALTER TABLE ... SET TBLPROPERTIES` might behave differently. **Low risk — Iceberg DDL is confirmed to work on S3TB by AWS docs.**

### 3. `dbt clean` / `dbt deps` / `dbt debug` with S3TB profile

These commands may call adapter methods that hit `_get_data_catalog`. If the S3TB named catalog doesn't exist yet (user hasn't registered it), `get_data_catalog(Name=...)` will throw `InvalidRequestException`. The existing error handling in `_get_data_catalog` doesn't catch this. **Low risk — these commands rarely exercise catalog methods. But `dbt debug` specifically tests connection, and could fail unhelpfully.**

### 4. Concurrent dbt runs against same S3TB namespace

Drop-and-recreate has a downtime window. If two dbt runs execute simultaneously, one could drop a table that the other is about to query or merge into. **This is inherent to drop-and-recreate and not specific to S3TB — same risk exists for Hive non-HA tables. Document as known limitation.**

### 5. `persist_docs` on S3TB tables

`persist_docs_to_glue` calls `glue.update_table()` with `CatalogId`. For S3TB, this would try to update the Glue table metadata (description, column descriptions). Whether Glue allows metadata updates on federated S3TB tables is untested. **Low risk — `persist_docs` is optional and off by default.**

---

## Dependencies

### External — will not change

- **AWS S3 Tables API**: GA since Dec 2024. Stable.
- **Athena S3TB integration**: GA. SQL syntax documented.
- **Glue API with compound CatalogId**: Verified working for all tested operations.
- **pyathena**: No changes needed. Current version supports catalog_name pass-through.
- **boto3/botocore**: Current versions support `s3tables` and compound `CatalogId`. No version bump needed.
- **moto**: Does NOT support S3 Tables catalogs. Tests must use `botocore._make_api_call` patching pattern.

### Internal — could affect implementation

- **dbt-core / dbt-common**: No dependency on specific versions for this feature. The macro dispatch and `@available` decorator are stable.
- **dbt-adapters base**: No changes needed to the base adapter.
- **Pre-existing bugs found**: `_get_glue_table_versions_to_expire` missing `CatalogId`, `list_schemas` missing `CatalogId`, `swap_table` reuses src catalog for target. These are NOT blockers for S3TB — the first two are addressed in our tasks, the third is Hive-only and irrelevant.

---

## Recommendation

**Ready to implement with these amendments to the tasks:**

1. **T-2, T-4, T-5, T-6**: Use the `config.get('table_type')` (no default) pattern to distinguish explicit hive from unset. Don't use `config.get('table_type', default='hive')`.

2. **T-4**: Explicitly handle the `old_relation.is_view` sub-case (drop view, create table — no rename).

3. **T-5**: Add `is_s3tb` guard around `expire_glue_table_versions` call in incremental.sql.

4. **T-2**: Add `is_s3tb` guard for `force_batch=True` (raise error — batched partition inserts are Hive-only).

5. **T-6**: View guard goes in `materializations/models/view/view.sql` at the top of `{% materialization view, adapter='athena' %}`.

6. **T-8/T-9**: Add test case for multi-catalog project (some models regular Athena, some S3TB).

No architectural changes needed. The plan is sound. The biggest risk (incremental merge temp tables in S3TB) is functionally correct — just needs performance validation during T-9 integration testing.
