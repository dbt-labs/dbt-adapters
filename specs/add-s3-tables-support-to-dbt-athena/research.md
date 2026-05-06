# Research Items: S3 Table Bucket Support

Technical unknowns that need verification before or during implementation. These should be tested against the real S3 Table Bucket (`arn:aws:s3tables:eu-north-1:182399687476:bucket/dbt-athena-test`) in `ida-prod`.

## R-1: Namespace Creation via SQL

**Question**: Does Athena's `CREATE SCHEMA IF NOT EXISTS <namespace>` work when the connection's catalog is an S3 Table Bucket?

**Context**: The `athena__create_schema` macro runs `CREATE SCHEMA IF NOT EXISTS` via Athena SQL. Since pyathena sets `catalog_name=creds.database` (the S3TB catalog name), the DDL should target the correct catalog. But AWS documentation doesn't explicitly confirm this path.

**Fallback**: If SQL schema creation fails, implement a Python-side override that calls `glue.create_database(CatalogId="<account>:s3tablescatalog/<bucket>", DatabaseInput={"Name": "<namespace>"})` instead.

**Test**:
```bash
AWS_VAULT= aws-vault exec ida-prod -- aws athena start-query-execution \
  --query-string "CREATE SCHEMA IF NOT EXISTS test_namespace" \
  --work-group primary \
  --query-execution-context Catalog=<s3tb-catalog-name> \
  --result-configuration OutputLocation=s3://<staging-bucket>/athena-results/
```

Or via Glue API:
```bash
AWS_VAULT= aws-vault exec ida-prod -- aws glue create-database \
  --catalog-id "182399687476:s3tablescatalog/dbt-athena-test" \
  --database-input '{"Name": "test_namespace"}'
```

## R-2: Table Deletion via Glue API

**Question**: Does `glue.delete_table(CatalogId="<account>:s3tablescatalog/<bucket>", DatabaseName="<namespace>", Name="<table>")` successfully delete an S3 Table Bucket table and its managed data?

**Context**: The adapter currently drops tables via either SQL `DROP TABLE` or `glue.delete_table()`. SQL DROP is blocked by AWS for S3TB. We plan to use `glue.delete_table()` with the compound CatalogId. Need to verify this actually works and cleans up managed storage.

**Fallback**: If Glue's `delete_table` doesn't work, use the S3 Tables API: `s3tables.delete_table(tableBucketARN=..., namespace=..., name=...)`. This requires adding `boto3.client('s3tables')` and the `s3tables` type stubs.

**Test**: Create a test table via Athena CTAS, then delete via Glue API:
```bash
# Create
AWS_VAULT= aws-vault exec ida-prod -- aws athena start-query-execution \
  --query-string "CREATE TABLE \"<catalog>\".\"test_namespace\".\"test_delete\" WITH (format='PARQUET') AS SELECT 1 AS id" \
  --work-group primary \
  --result-configuration OutputLocation=s3://<staging-bucket>/athena-results/

# Delete via Glue
AWS_VAULT= aws-vault exec ida-prod -- aws glue delete-table \
  --catalog-id "182399687476:s3tablescatalog/dbt-athena-test" \
  --database-name test_namespace \
  --name test_delete

# Verify deletion
AWS_VAULT= aws-vault exec ida-prod -- aws glue get-table \
  --catalog-id "182399687476:s3tablescatalog/dbt-athena-test" \
  --database-name test_namespace \
  --name test_delete
```

## R-3: Glue Table Metadata Format for S3TB Tables

**Question**: Does `glue.get_table()` return the same metadata structure for S3TB Iceberg tables as for regular Iceberg tables? Specifically:
- Is `Table.Parameters.table_type` set to `"iceberg"`?
- Is `Table.TableType` set to `"EXTERNAL_TABLE"` or something else?
- Does `Table.StorageDescriptor.Location` exist and what format is it?

**Context**: The adapter's `get_table_type()` function detects Iceberg by checking `Parameters.table_type == "iceberg"`. If S3TB tables have different metadata, this detection breaks.

**Test**:
```bash
AWS_VAULT= aws-vault exec ida-prod -- aws glue get-table \
  --catalog-id "182399687476:s3tablescatalog/dbt-athena-test" \
  --database-name <namespace> \
  --name <existing-table> | jq '.Table | {TableType, Parameters, StorageDescriptor: {Location: .StorageDescriptor.Location}}'
```

## R-4: list_schemas / get_databases with Compound CatalogId

**Question**: Does `glue.get_databases(CatalogId="<account>:s3tablescatalog/<bucket>")` return namespaces in the S3 Table Bucket?

**Context**: `list_schemas()` in `impl.py` currently doesn't pass `CatalogId` at all (existing bug). The fix is to pass the compound CatalogId. Need to verify the Glue API supports this.

**Test**:
```bash
AWS_VAULT= aws-vault exec ida-prod -- aws glue get-databases \
  --catalog-id "182399687476:s3tablescatalog/dbt-athena-test"
```

## R-5: Athena Named Catalog Registration

**Question**: Verify the exact `create-data-catalog` command works and the resulting catalog is queryable.

**Context**: The prerequisite step users must perform. Need to confirm the exact command, verify `GetDataCatalog` returns the expected structure, and confirm queries work.

**Test**:
```bash
# Register (if not already done)
AWS_VAULT= aws-vault exec ida-prod -- aws athena create-data-catalog \
  --name dbt-athena-s3tb-test \
  --type GLUE \
  --parameters catalog-id=182399687476:s3tablescatalog/dbt-athena-test

# Verify
AWS_VAULT= aws-vault exec ida-prod -- aws athena get-data-catalog \
  --name dbt-athena-s3tb-test

# Test query
AWS_VAULT= aws-vault exec ida-prod -- aws athena start-query-execution \
  --query-string "SHOW DATABASES" \
  --work-group primary \
  --query-execution-context Catalog=dbt-athena-s3tb-test \
  --result-configuration OutputLocation=s3://<staging-bucket>/athena-results/
```

## R-6: CTAS Syntax Verification

**Question**: Verify the exact CTAS syntax that works for S3TB tables — specifically confirming that omitting `table_type`, `is_external`, and `location` produces a valid statement.

**Test**:
```bash
AWS_VAULT= aws-vault exec ida-prod -- aws athena start-query-execution \
  --query-string "CREATE TABLE \"dbt-athena-s3tb-test\".\"test_namespace\".\"ctas_test\" WITH (format='PARQUET') AS SELECT 1 AS id, 'hello' AS name" \
  --work-group primary \
  --result-configuration OutputLocation=s3://<staging-bucket>/athena-results/
```

## Results (verified 2026-04-23)

All items tested against `arn:aws:s3tables:eu-north-1:004956439775:bucket/dbt-athena-test` in `ida-prod`. Named Athena catalog: `dbt_athena_s3tb_test`.

| Item | Result | Key Findings |
|------|--------|-------------|
| R-5 | ✅ | `GetDataCatalog` returns `Type: "GLUE"`, `Parameters: {"catalog-id": "004956439775:s3tablescatalog/dbt-athena-test"}`, `Status: "CREATE_COMPLETE"` |
| R-6 | ✅ | `WITH (format='PARQUET')` only — no location/table_type/is_external. Partitioning also works: `partitioning=ARRAY['id']` |
| R-2 | ✅ | `glue.delete_table(CatalogId=compound_id, ...)` works. Table confirmed gone via `get_table` returning `EntityNotFoundException` |
| R-3 | ✅ | `Parameters.table_type = "ICEBERG"` (uppercase — `.lower()` handles it). `TableType = "customer"` (not in relation map, but Iceberg check runs first). `StorageDescriptor.Location` = managed S3 path (`s3://<uuid>--table-s3`). `FederatedTable.ConnectionType = "aws:s3tables"`. Full column metadata with `iceberg.field.*` params present. |
| R-1 | ✅ | Both `glue.create_database(CatalogId=compound_id)` and SQL `CREATE SCHEMA IF NOT EXISTS` (with `Catalog` context) work |
| R-4 | ✅ | `glue.get_databases(CatalogId=compound_id)` returns namespaces correctly |

### No fallbacks needed

All planned approaches work as designed. No `s3tables` boto3 client required. No Python-side schema creation override needed.
