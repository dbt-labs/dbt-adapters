# dbt-bigquery Adapter Development

Development guide specific to the dbt-bigquery adapter.

## Overview

dbt-bigquery provides Google BigQuery-specific functionality including partitioning, clustering, BigQuery ML integration, Iceberg tables, and service account authentication.

## Adapter Architecture

```
dbt-bigquery/
├── src/dbt/
│   ├── adapters/bigquery/
│   │   ├── __init__.py              # Plugin registration
│   │   ├── __version__.py           # Version info
│   │   ├── connections.py           # BigQueryConnectionManager
│   │   ├── credentials.py           # BigQueryCredentials
│   │   ├── impl.py                  # BigQueryAdapter
│   │   ├── relation.py              # BigQueryRelation
│   │   ├── column.py                # BigQueryColumn (STRUCT support)
│   │   ├── clients.py               # Custom BigQuery clients
│   │   ├── dataset.py               # Dataset management
│   │   ├── parse_model.py           # BigQuery model parsing
│   │   ├── python_submissions.py    # BigQuery ML/Python execution
│   │   ├── token_suppliers.py       # Service account token refresh
│   │   ├── struct_utils.py          # STRUCT type utilities
│   │   ├── retry.py                 # API retry logic
│   │   └── catalogs/                # BigQuery catalog support
│   └── include/bigquery/
│       └── macros/                  # BigQuery SQL implementations
├── tests/
│   ├── unit/                        # Unit tests
│   └── functional/                  # Integration tests
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

## Key Components

### BigQueryAdapter (`impl.py`)

Extends `BaseAdapter` (not SQLAdapter) with BigQuery-specific features:

- **Partitioning**: Time and integer range partitioning
- **Clustering**: Multi-column clustering
- **BigQuery ML**: ML model training and prediction
- **Python Models**: Dataproc/Spark execution
- **Iceberg Tables**: Apache Iceberg format support

### BigQueryConnectionManager (`connections.py`)

Uses `google-cloud-bigquery` client with support for:

- Service account authentication
- OAuth authentication
- Application default credentials
- Impersonation
- Location-based execution

### BigQueryCredentials (`credentials.py`)

Key credential fields:

```python
project: str                      # GCP project ID
dataset: str                      # Default dataset
location: str                     # BigQuery location (US, EU, etc.)
keyfile: str                      # Path to service account JSON
keyfile_json: dict                # Inline service account JSON
method: str                       # Auth method (oauth, service-account)
impersonate_service_account: str  # Service account to impersonate
execution_project: str            # Project for query execution
maximum_bytes_billed: int         # Query cost limit
priority: str                     # Query priority (INTERACTIVE, BATCH)
timeout_seconds: int              # Query timeout
```

### BigQueryRelation (`relation.py`)

Supports BigQuery-specific features:

- Dataset as schema concept
- Partitioned tables
- Clustered tables
- External tables
- Table cloning

### BigQueryColumn (`column.py`)

Handles BigQuery-specific types:

- STRUCT (nested records)
- ARRAY
- GEOGRAPHY
- JSON

## Development Setup

### Initial Setup

```shell
cd dbt-bigquery
hatch run setup
```

### Environment Shell

```shell
hatch shell
```

## Test Environment Configuration

Create `test.env` from `test.env.example`:

### Service Account Authentication

```shell
BIGQUERY_TEST_SERVICE_ACCOUNT_JSON='{"type": "service_account", "project_id": "...", ...}'
```

### Test Resources

```shell
BIGQUERY_TEST_NO_ACCESS_DATABASE=project_without_access
BIGQUERY_TEST_ICEBERG_BUCKET=gs://my-iceberg-bucket
```

### Test Users (Groups/Service Accounts)

```shell
DBT_TEST_USER_1="group:buildbot@dbtlabs.com"
DBT_TEST_USER_2="group:engineering-core-team@dbtlabs.com"
DBT_TEST_USER_3="serviceAccount:dbt-integration-test-user@dbt-test-env.iam.gserviceaccount.com"
```

### Python Model Testing (Dataproc)

```shell
COMPUTE_REGION=us-central1
DATAPROC_CLUSTER_NAME=my-dataproc-cluster
GCS_BUCKET=gs://my-temp-bucket
```

## Running Tests

### Unit Tests

```shell
# All unit tests
hatch run unit-tests

# Specific file
hatch run unit-tests tests/unit/test_bigquery_adapter.py

# Specific test
hatch run unit-tests tests/unit/test_bigquery_adapter.py::TestClass::test_method
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

## BigQuery-Specific Features

### Time Partitioning

```sql
{{ config(
    materialized='table',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
) }}
```

### Integer Range Partitioning

```sql
{{ config(
    materialized='table',
    partition_by={
      "field": "user_id",
      "data_type": "int64",
      "range": {"start": 0, "end": 1000, "interval": 10}
    },
) }}
```

### Clustering

```sql
{{ config(
    materialized='table',
    cluster_by=['customer_id', 'order_date'],
) }}
```

### Query Cost Control

```sql
{{ config(
    materialized='table',
    maximum_bytes_billed=1000000000,  -- 1GB limit
) }}
```

### Labels

```sql
{{ config(
    materialized='table',
    labels={'team': 'analytics', 'env': 'prod'},
) }}
```

### Copy Partitions

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date_column', 'data_type': 'date'},
    copy_partitions=true,
) }}
```

### Iceberg Tables

```sql
{{ config(
    materialized='table',
    table_format='iceberg',
) }}
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with partitioning, clustering |
| `connections.py` | BigQuery client management |
| `credentials.py` | GCP authentication handling |
| `relation.py` | Dataset/table relation handling |
| `column.py` | STRUCT and nested type support |
| `python_submissions.py` | Dataproc/Spark job execution |
| `retry.py` | API retry and rate limiting |

## Available Hatch Scripts

| Command | Description |
|---------|-------------|
| `hatch run setup` | Initial development setup |
| `hatch run code-quality` | Run pre-commit on all files |
| `hatch run unit-tests` | Run unit tests |
| `hatch run integration-tests` | Run functional tests |

## Dependencies

- **google-cloud-bigquery** - BigQuery Python client
- **google-cloud-storage** - GCS for staging
- **google-cloud-dataproc** - Dataproc for Python models
- **dbt-adapters**, **dbt-common**, **dbt-core** - Core dbt packages

## Best Practices

1. Use `maximum_bytes_billed` to prevent expensive queries during testing
2. Test partitioning and clustering configurations thoroughly
3. Validate service account permissions for integration tests
4. Test STRUCT/nested type handling when modifying column code
5. Consider location requirements for datasets and queries
6. Use batch priority for non-urgent test queries to reduce costs
