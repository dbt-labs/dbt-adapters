# dbt-athena Adapter Development

Development guide specific to the dbt-athena adapter.

## Overview

dbt-athena provides AWS Athena-specific functionality including S3 data lake integration, Glue catalog support, Lake Formation permissions, and Iceberg table format.

## Adapter Architecture

```
dbt-athena/
├── src/dbt/
│   ├── adapters/athena/
│   │   ├── __init__.py              # Plugin registration
│   │   ├── __version__.py           # Version info
│   │   ├── connections.py           # AthenaConnectionManager
│   │   ├── impl.py                  # AthenaAdapter
│   │   ├── relation.py              # AthenaRelation
│   │   ├── column.py                # AthenaColumn
│   │   ├── config.py                # Athena-specific config
│   │   ├── session.py               # Session management
│   │   ├── s3.py                    # S3 integration
│   │   ├── lakeformation.py         # Lake Formation integration
│   │   └── python_submissions.py    # PySpark on Athena
│   └── include/athena/
│       └── macros/                  # Athena SQL implementations
├── tests/
│   ├── unit/                        # Unit tests
│   └── functional/                  # Integration tests
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

## Key Components

### AthenaAdapter (`impl.py`)

Extends `SQLAdapter` with Athena-specific features:

- **S3 Data Lake**: Direct S3 table storage
- **Glue Catalog**: AWS Glue metastore integration
- **Iceberg Tables**: Apache Iceberg format support
- **Lake Formation**: Fine-grained access control
- **Workgroups**: Query workgroup management

### AthenaConnectionManager (`connections.py`)

Uses `pyathena` driver with support for:

- AWS credentials (access key, profile, role)
- Workgroup configuration
- S3 staging directory
- Query result caching
- Poll interval configuration

### AthenaCredentials (`connections.py`)

Key credential fields:

```python
s3_staging_dir: str          # S3 location for query results
s3_tmp_table_dir: str        # S3 location for temp tables
region_name: str             # AWS region
database: str                # Glue database name
schema: str                  # Schema (alias for database)
work_group: str              # Athena workgroup
aws_profile_name: str        # AWS profile
aws_access_key_id: str       # AWS access key
aws_secret_access_key: str   # AWS secret key
poll_interval: float         # Query status poll interval
num_retries: int             # Number of retries
threads: int                 # Concurrent threads
```

### AthenaRelation (`relation.py`)

Supports Athena-specific features:

- External tables with S3 location
- Partitioned tables (Hive-style)
- Iceberg tables
- Table properties

## Development Setup

### Initial Setup

```shell
cd dbt-athena
hatch run setup
```

### Environment Shell

```shell
hatch shell
```

## Test Environment Configuration

Create `test.env` from `test.env.example`:

### AWS Connection

```shell
DBT_TEST_ATHENA_S3_STAGING_DIR=s3://your-bucket/athena-results/
DBT_TEST_ATHENA_S3_TMP_TABLE_DIR=s3://your-bucket/athena-tmp/
DBT_TEST_ATHENA_REGION_NAME=us-east-1
DBT_TEST_ATHENA_DATABASE=your_database
DBT_TEST_ATHENA_SCHEMA=your_schema
DBT_TEST_ATHENA_WORK_GROUP=primary
DBT_TEST_ATHENA_AWS_PROFILE_NAME=your-aws-profile
```

### Query Settings

```shell
DBT_TEST_ATHENA_THREADS=4
DBT_TEST_ATHENA_POLL_INTERVAL=1
DBT_TEST_ATHENA_NUM_RETRIES=3
```

## Running Tests

### Unit Tests

```shell
# All unit tests
hatch run unit-tests

# Specific file
hatch run unit-tests tests/unit/test_athena_adapter.py

# Specific test
hatch run unit-tests tests/unit/test_athena_adapter.py::TestClass::test_method
```

### Integration Tests

```shell
hatch run integration-tests
```

## Code Quality

```shell
hatch run code-quality
```

## Changelog

```shell
changie new
```

## Athena-Specific Features

### External Tables

```sql
{{ config(
    materialized='table',
    external_location='s3://bucket/path/to/data/',
    format='parquet',
) }}
```

### Partitioning

```sql
{{ config(
    materialized='table',
    partitioned_by=['year', 'month'],
    format='parquet',
) }}
```

### Iceberg Tables

```sql
{{ config(
    materialized='table',
    table_type='iceberg',
    format='parquet',
) }}
```

### Table Properties

```sql
{{ config(
    materialized='table',
    tblproperties={
      'has_encrypted_data': 'false',
      'classification': 'json',
    },
) }}
```

### Workgroup

```sql
{{ config(
    materialized='table',
    work_group='analytics_team',
) }}
```

### CTAS vs Insert

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['date_column'],
) }}
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with S3, Glue integration |
| `connections.py` | PyAthena connection handling |
| `relation.py` | S3 location, partition configs |
| `column.py` | Athena SQL type handling |
| `s3.py` | S3 operations (delete, list) |
| `lakeformation.py` | Lake Formation permission management |

## Available Hatch Scripts

| Command | Description |
|---------|-------------|
| `hatch run setup` | Initial development setup |
| `hatch run code-quality` | Run pre-commit on all files |
| `hatch run unit-tests` | Run unit tests |
| `hatch run integration-tests` | Run functional tests |

## Dependencies

- **pyathena** - Athena Python driver
- **boto3** - AWS SDK for S3, Glue operations
- **dbt-adapters**, **dbt-common**, **dbt-core** - Core dbt packages

## Best Practices

1. Use dedicated S3 buckets for testing (not production data)
2. Test with both Hive and Iceberg table formats
3. Validate Lake Formation permissions when modifying access code
4. Consider query costs and workgroup quotas
5. Test partition operations with realistic data volumes
6. Clean up S3 staging directories after tests
