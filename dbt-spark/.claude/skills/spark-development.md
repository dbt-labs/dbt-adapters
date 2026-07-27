# dbt-spark Adapter Development

Development guide specific to the dbt-spark adapter.

## Overview

dbt-spark provides Apache Spark and Databricks-specific functionality including Delta Lake support, Hive metastore integration, and multiple connection methods (Thrift, ODBC, Databricks SQL).

## Adapter Architecture

```
dbt-spark/
├── src/dbt/
│   ├── adapters/spark/
│   │   ├── __init__.py              # Plugin registration
│   │   ├── __version__.py           # Version info
│   │   ├── connections.py           # SparkConnectionManager
│   │   ├── impl.py                  # SparkAdapter
│   │   ├── relation.py              # SparkRelation
│   │   ├── column.py                # SparkColumn
│   │   ├── session.py               # Spark session management
│   │   └── python_submissions.py    # PySpark job submission
│   └── include/spark/
│       └── macros/                  # Spark SQL implementations
├── tests/
│   ├── unit/                        # Unit tests
│   └── functional/                  # Integration tests
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

## Key Components

### SparkAdapter (`impl.py`)

Extends `SQLAdapter` with Spark-specific features:

- **Delta Lake**: Native Delta table support
- **Hive Metastore**: HMS integration
- **File Formats**: Parquet, Delta, Hudi, Iceberg
- **Partitioning**: Hive-style partitioning
- **Python Models**: PySpark execution

### SparkConnectionManager (`connections.py`)

Supports multiple connection methods:

- **Thrift**: Direct connection to Spark Thrift Server
- **ODBC**: Databricks ODBC driver
- **Databricks SQL**: Databricks SQL connector
- **HTTP**: Databricks HTTP endpoint

### SparkCredentials (`connections.py`)

Key credential fields:

```python
# Connection method
method: str                  # thrift, odbc, http, databricks

# Thrift connection
host: str                    # Spark Thrift Server host
port: int = 10001            # Thrift port

# Databricks connection
host: str                    # Databricks workspace URL
token: str                   # Personal access token
cluster: str                 # Cluster ID or SQL endpoint
http_path: str               # HTTP path for SQL endpoint
endpoint: str                # SQL warehouse endpoint

# Common settings
schema: str                  # Default schema/database
connect_timeout: int         # Connection timeout
connect_retries: int         # Number of retry attempts
```

### SparkRelation (`relation.py`)

Supports Spark-specific features:

- Hive-style partitioning
- File format specification
- Delta Lake properties
- External tables with location

## Development Setup

### Initial Setup

```shell
cd dbt-spark
hatch run setup
```

### Environment Shell

```shell
hatch shell
```

## Test Environment Configuration

Create `test.env` from `test.env.example`:

### Databricks Connection

```shell
DBT_DATABRICKS_HOST_NAME=your-workspace.cloud.databricks.com
DBT_DATABRICKS_TOKEN=dapi_your_personal_token
DBT_DATABRICKS_CLUSTER_NAME=sql/protocolv1/o/xxx/yyy
DBT_DATABRICKS_ENDPOINT=/sql/1.0/warehouses/abc123
ODBC_DRIVER=/path/to/simba/spark/odbc/driver
```

### Test Users

```shell
DBT_TEST_USER_1=user1@example.com
DBT_TEST_USER_2=user2@example.com
DBT_TEST_USER_3=user3@example.com
```

## Running Tests

### Unit Tests

```shell
# All unit tests
hatch run unit-tests

# Specific file
hatch run unit-tests tests/unit/test_spark_adapter.py

# Specific test
hatch run unit-tests tests/unit/test_spark_adapter.py::TestClass::test_method
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

## Spark-Specific Features

### Delta Lake Tables

```sql
{{ config(
    materialized='table',
    file_format='delta',
) }}
```

### Partitioning

```sql
{{ config(
    materialized='table',
    partition_by=['year', 'month'],
    file_format='parquet',
) }}
```

### Bucketing

```sql
{{ config(
    materialized='table',
    buckets=8,
    bucket_columns=['user_id'],
) }}
```

### Location

```sql
{{ config(
    materialized='table',
    location='s3://bucket/path/to/table',
) }}
```

### Incremental with Merge

```sql
{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key='id',
) }}
```

### Table Properties

```sql
{{ config(
    materialized='table',
    tblproperties={
      'delta.autoOptimize.optimizeWrite': 'true',
      'delta.autoOptimize.autoCompact': 'true',
    },
) }}
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with Delta, Hive support |
| `connections.py` | Multi-method connection handling |
| `relation.py` | Partitioning, file format configs |
| `column.py` | Spark SQL type handling |
| `session.py` | Spark session management |
| `python_submissions.py` | PySpark job execution |

## Available Hatch Scripts

| Command | Description |
|---------|-------------|
| `hatch run setup` | Initial development setup |
| `hatch run code-quality` | Run pre-commit on all files |
| `hatch run unit-tests` | Run unit tests |
| `hatch run integration-tests` | Run functional tests |

## Dependencies

- **pyhive** - Thrift connection support
- **thrift** - Apache Thrift protocol
- **databricks-sql-connector** - Databricks SQL connection
- **pyodbc** - ODBC driver support (optional)
- **dbt-adapters**, **dbt-common**, **dbt-core** - Core dbt packages

## Best Practices

1. Test with multiple connection methods (Thrift, Databricks SQL)
2. Validate Delta Lake operations on actual Spark cluster
3. Test partitioning with various partition schemes
4. Consider cluster startup time in integration tests
5. Use SQL warehouses for faster test execution on Databricks
6. Test both managed and external table scenarios
