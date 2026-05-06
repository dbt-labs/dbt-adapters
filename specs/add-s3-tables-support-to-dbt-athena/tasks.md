# Implementation Tasks: S3 Table Bucket Support for dbt-athena

All research items verified against `arn:aws:s3tables:eu-north-1:004956439775:bucket/dbt-athena-test` in `ida-prod`. Named Athena catalog: `dbt_athena_s3tb_test`.

## Setup

- [x] **T-0: Branch and environment setup**
  - Check out feature branch `add-s3-tables-support-to-dbt-athena` from `upstream/main`
  - Run `cd dbt-athena && hatch run setup`
  - Verify existing tests pass: `hatch run unit-tests`

## Python Foundation

- [x] **T-1: Detection mechanism and list_schemas fix** (`impl.py`)
  - Add `@lru_cache` to `_get_data_catalog(self, database: str)` to eliminate redundant API calls (currently called 14+ times uncached)
  - Add `@available` method `is_s3_table_bucket(self, database: str) -> bool`:
    - Calls `_get_data_catalog(database)`
    - Returns `True` when catalog is `Type == "GLUE"` and `Parameters.get("catalog-id", "")` contains `"s3tablescatalog/"`
    - Returns `False` for `None`, non-GLUE types, or GLUE catalogs without `s3tablescatalog/` in catalog-id
  - Fix `list_schemas(self, database: str)`:
    - Call `_get_data_catalog(database)` and `get_catalog_id(data_catalog)`
    - Pass `CatalogId=catalog_id` to `glue.get_databases()` paginator when truthy (same pattern as `check_schema_exists`)
  - **Verify**: Write and run unit tests for `is_s3_table_bucket()` with mocked catalog responses: S3TB GLUE, regular GLUE, LAMBDA, HIVE, None, empty database string

## Macro Changes — Core DDL

- [x] **T-2: CTAS DDL changes** (`create_table_as.sql`)
  - Set `is_s3tb = adapter.is_s3_table_bucket(relation.database)` near the top
  - Use `config.get('table_type')` **without a default** to distinguish explicit hive from unset. Logic: if `is_s3tb` and raw value is `'hive'` → `CompilationError`; if `is_s3tb` and unset → default to `'iceberg'`; if not S3TB and unset → default to `'hive'`
  - Apply same `table_type` resolution pattern in `table.sql`, `incremental.sql`, `snapshot.sql` (T-4, T-5, T-6)
  - When `is_s3tb` and `language == 'python'`: raise `CompilationError` ("Python models targeting S3 Table Buckets are not yet supported.")
  - When `is_s3tb`: skip `adapter.generate_s3_location()` call (set `location` to `none`)
  - When `is_s3tb`: skip `adapter.delete_from_s3(location)` call
  - When `is_s3tb`: skip the `error_unique_location_iceberg` validation (irrelevant — no RENAME used)
  - When `is_s3tb`: in the SQL CTAS `WITH (...)` clause, omit `table_type=`, `is_external=`, and `location=`/`external_location=` properties. Emit only `format` + optional `partitioning` + optional `table_properties`
  - When `is_s3tb` and `force_batch=True`: raise `CompilationError` (batched partition inserts use Hive-style DDL, incompatible with S3TB)
  - Non-S3TB paths remain completely unchanged
  - **Verify**: Inspect generated DDL by reading the macro; unit test in T-8

- [x] **T-3: Drop path changes** (`relation.sql`)
  - `athena__drop_relation`: when `adapter.is_s3_table_bucket(relation.database)`:
    - Skip `clean_up_table(relation)` (no S3 object deletion on managed storage)
    - Call `adapter.delete_from_glue_catalog(relation)` directly
    - If `native_drop` is set, log a warning: "native_drop is ignored for S3 Table Bucket targets — SQL DROP TABLE is not supported by AWS. Using Glue API deletion."
  - `athena__rename_relation`: add defensive guard at top — if `adapter.is_s3_table_bucket(from_relation.database)`, raise `CompilationError` ("ALTER TABLE RENAME is not supported on S3 Table Bucket catalogs by AWS.")
  - Non-S3TB paths remain completely unchanged
  - **Verify**: Code review; unit test in T-8

## Macro Changes — Materializations

- [x] **T-4: Table materialization** (`table.sql`)
  - Set `is_s3tb` flag after reading `table_type` config
  - When `is_s3tb`: default `table_type` to `'iceberg'`
  - When `is_s3tb` and `language == 'python'`: raise `CompilationError`
  - When `is_s3tb` and `old_relation is not none` (re-run): use drop-and-recreate instead of rename-swap:
    - Call `drop_relation(old_relation)` (which routes to Glue API deletion via T-3)
    - Then `safe_create_table_as(False, target_relation, compiled_code, language, force_batch)`
    - Skip all rename logic, backup relation creation, and HA path
    - **Also handle the `old_relation.is_view` sub-case**: drop the view, then create table directly (no rename needed)
  - When `is_s3tb` and `old_relation is none` (first run): `safe_create_table_as` directly to target (same as current iceberg first-run path sans location)
  - Non-S3TB paths remain completely unchanged
  - **Verify**: Code review; integration test in T-9

- [x] **T-5: Incremental materialization** (`incremental.sql`)
  - Set `is_s3tb` flag after reading `table_type` config
  - When `is_s3tb`: default `table_type` to `'iceberg'`
  - When `is_s3tb` and `language == 'python'`: raise `CompilationError`
  - When `is_s3tb` and `should_full_refresh()`: use drop-and-recreate instead of rename-swap (same pattern as T-4)
  - Normal incremental paths (merge, append) are unchanged — they use `MERGE INTO` / `INSERT INTO` SQL, no location or rename involved
  - Ensure the full-refresh path for S3TB doesn't enter the existing `'unique' not in s3_data_naming or external_location is not none` branch (which triggers rename-swap)
  - Guard `expire_glue_table_versions` call: skip when `is_s3tb` (S3TB manages version lifecycle automatically; the underlying `_get_glue_table_versions_to_expire` also doesn't pass CatalogId)
  - Non-S3TB paths remain completely unchanged
  - **Verify**: Code review; integration test in T-9

- [x] **T-6: Snapshot materialization and error guards** (`snapshot.sql`, `seeds/helpers.sql`, view macro)
  - **Snapshot** (`snapshot.sql`): when `is_s3tb`, default `table_type` to `'iceberg'`; raise `CompilationError` if `table_type='hive'`. Iceberg snapshot path uses MERGE INTO (works unchanged). Initial creation flows through fixed `create_table_as` from T-2.
  - **Seeds** (`seeds/helpers.sql`): at top of `athena__create_csv_table`, check `adapter.is_s3_table_bucket(model.database)` — raise `CompilationError` ("Seeds are not supported for S3 Table Bucket targets. Load seed data through an alternative method.")
  - **Views**: add guard at top of `materializations/models/view/view.sql` (`{% materialization view, adapter='athena' %}`), checking `adapter.is_s3_table_bucket(database)` where `database` is the template-scope variable. Raise `CompilationError` ("CREATE VIEW is not supported on S3 Table Bucket catalogs by AWS.")
  - **Verify**: Code review; unit test error messages in T-8

## Testing

- [x] **T-7: Changelog entry**
  - Run `cd dbt-athena && changie new`
  - Category: `Features`
  - Description: "Add support for AWS S3 Table Buckets as materialization targets. Users can configure a named Athena data catalog backed by an S3 Table Bucket and materialize table, incremental (merge/append), and snapshot models."

- [x] **T-8: Unit tests**
  - Create `tests/unit/test_s3_table_bucket.py` (or add to `test_adapter.py`)
  - **Detection tests** (`is_s3_table_bucket`):
    - S3TB catalog (`Type: GLUE`, `catalog-id` with `s3tablescatalog/`) → `True`
    - Regular GLUE catalog (`catalog-id` without `s3tablescatalog/`) → `False`
    - LAMBDA catalog → `False`
    - HIVE catalog → `False`
    - `None` database → `False`
    - Empty string database → `False`
  - **list_schemas tests**: verify `CatalogId` is passed to `glue.get_databases()` paginator
  - **DDL generation tests** (Jinja2 template rendering, follow `test_get_partition_batches.py` pattern):
    - S3TB CTAS: no `table_type`, `is_external`, or `location` in WITH clause
    - S3TB CTAS: `format` and `partitioning` present
    - Non-S3TB CTAS: unchanged (regression)
  - **Error guard tests**: seeds, views, Python models raise `CompilationError` for S3TB
  - Use `botocore.client.BaseClient._make_api_call` patching for `GetDataCatalog` mocking (moto doesn't support S3TB-style catalogs)
  - **Multi-catalog test**: verify that a project with some models on regular Athena and others on S3TB (via `database` config override) works correctly — `is_s3_table_bucket` returns different results per relation
  - **Verify**: `hatch run unit-tests`

- [x] **T-9: Integration tests**
  - Create `tests/functional/test_s3_table_bucket.py`
  - Requires `test.env` with Athena credentials for `ida-prod` (eu-north-1) and S3TB catalog name `dbt_athena_s3tb_test`
  - **Test cases**:
    - Table materialization: first run (create), second run (drop-and-recreate)
    - Incremental merge: first run + incremental run
    - Incremental append: first run + incremental run
    - Full refresh (`--full-refresh` flag)
    - Snapshot: initial + subsequent
    - Seed → clear error
    - View → clear error
  - **Verify**: `hatch run integration-tests -- tests/functional/test_s3_table_bucket.py -v`

## Finalization

- [x] **T-10: Code quality and regression check**
  - `hatch run code-quality` — fix any Black/Flake8/MyPy issues
  - `hatch run unit-tests` — all existing + new tests pass
  - Verify no changes to non-S3TB code paths (diff review)
  - Update spec.md with resolved open questions (namespace creation ✅ works via SQL)
