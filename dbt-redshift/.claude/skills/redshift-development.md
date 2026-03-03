# dbt-redshift Adapter Development

Development guide specific to the dbt-redshift adapter.

## Overview

dbt-redshift extends dbt-postgres to provide Amazon Redshift-specific functionality including distribution keys, sort keys, materialized views, and IAM authentication.

## Adapter Architecture

```
dbt-redshift/
├── src/dbt/
│   ├── adapters/redshift/
│   │   ├── __init__.py              # Plugin registration
│   │   ├── __version__.py           # Version info
│   │   ├── connections.py           # RedshiftConnectionManager, RedshiftCredentials
│   │   ├── impl.py                  # RedshiftAdapter
│   │   ├── relation.py              # RedshiftRelation
│   │   ├── auth_providers.py        # IAM, Identity Center auth
│   │   ├── utility.py               # Helper functions
│   │   └── relation_configs/        # Materialized view, dist, sort configs
│   │       ├── base.py
│   │       ├── materialized_view.py
│   │       ├── dist.py
│   │       ├── sort.py
│   │       └── policies.py
│   └── include/redshift/
│       └── macros/                  # Redshift SQL implementations
├── tests/
│   ├── unit/                        # Unit tests
│   └── functional/                  # Integration tests
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

## Key Components

### RedshiftAdapter (`impl.py`)

Extends `PostgresAdapter` with Redshift-specific features:

- **Constraint Support**: NOT NULL enforced, others not supported
- **Capabilities**: Schema metadata, table last modified, catalog retrieval
- **Date Function**: `GETDATE()`
- **Type Conversions**: Redshift-specific type handling

### RedshiftConnectionManager (`connections.py`)

Uses `redshift_connector` library with support for:

- Basic authentication (host, port, user, password)
- IAM user authentication
- IAM role authentication
- Identity Center (browser) authentication
- SSL/TLS configuration
- Connection pooling

### RedshiftCredentials (`connections.py`)

Key credential fields:

```python
host: str                    # Cluster endpoint
port: int = 5439             # Default Redshift port
database: str                # Database name
user: str                    # Username
password: str                # Password (optional with IAM)
region: str                  # AWS region
cluster_id: str              # Cluster identifier (for IAM)
iam_profile: str             # IAM profile name
access_key_id: str           # IAM access key
secret_access_key: str       # IAM secret key
autocommit: bool = True      # Auto-commit transactions
connect_timeout: int = 30    # Connection timeout
```

### RedshiftRelation (`relation.py`)

Supports relation-specific configurations:

- Distribution styles: AUTO, EVEN, KEY, ALL
- Sort keys: COMPOUND, INTERLEAVED
- Materialized views with auto-refresh

### Auth Providers (`auth_providers.py`)

- `TokenAuthProvider` - Base token provider
- `IAMRoleAuthProvider` - Assume IAM role
- `BrowserIdentityCenterAuthProvider` - SSO via browser

## Development Setup

### Initial Setup

```shell
cd dbt-redshift
hatch run setup
```

This installs dbt-redshift, dbt-adapters, dbt-postgres, and dbt-tests-adapter in editable mode.

### Environment Shell

```shell
hatch shell
```

## Test Environment Configuration

Create `test.env` from `test.env.example`:

### Basic Authentication

```shell
REDSHIFT_TEST_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_TEST_PORT=5439
REDSHIFT_TEST_DBNAME=dev
REDSHIFT_TEST_USER=admin
REDSHIFT_TEST_PASS=your-password
REDSHIFT_TEST_REGION=us-east-1
```

### IAM Authentication

```shell
REDSHIFT_TEST_CLUSTER_ID=your-cluster-id

# IAM User Method
REDSHIFT_TEST_IAM_USER_PROFILE=your-profile
REDSHIFT_TEST_IAM_USER_ACCESS_KEY_ID=AKIA...
REDSHIFT_TEST_IAM_USER_SECRET_ACCESS_KEY=your-secret-key

# IAM Role Method
REDSHIFT_TEST_IAM_ROLE_PROFILE=your-role-profile
```

### Test Users

```shell
DBT_TEST_USER_1=dbt_test_user_1
DBT_TEST_USER_2=dbt_test_user_2
DBT_TEST_USER_3=dbt_test_user_3
```

## Running Tests

### Unit Tests

```shell
# All unit tests
hatch run unit-tests

# Specific file
hatch run unit-tests tests/unit/test_redshift_adapter.py

# Specific test
hatch run unit-tests tests/unit/test_redshift_adapter.py::TestClass::test_method
```

### Integration Tests

```shell
# All functional tests
hatch run integration-tests

# Flaky tests (run sequentially)
hatch run integration-tests-flaky
```

## Code Quality

```shell
hatch run code-quality
```

Runs Black, Flake8, and MyPy.

## Changelog

```shell
changie new
```

Categories: Breaking Changes, Features, Fixes, Under the Hood, Dependencies, Security

## Redshift-Specific Features

### Distribution Styles

```sql
{{ config(
    materialized='table',
    dist='key_column',          -- KEY distribution
    -- or dist='even'           -- EVEN distribution
    -- or dist='all'            -- ALL distribution
    -- or dist='auto'           -- AUTO distribution
) }}
```

### Sort Keys

```sql
{{ config(
    materialized='table',
    sort=['col1', 'col2'],              -- COMPOUND sort key
    sort_type='interleaved',            -- or 'compound' (default)
) }}
```

### Materialized Views

```sql
{{ config(
    materialized='materialized_view',
    auto_refresh=true,
    dist='column_name',
    sort=['col1'],
) }}
```

### Late Binding Views

```sql
{{ config(
    materialized='view',
    bind=false,  -- Creates late-binding view
) }}
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with constraints, capabilities, type conversions |
| `connections.py` | Connection management and credentials |
| `relation.py` | Relation handling with dist/sort configs |
| `auth_providers.py` | IAM and Identity Center authentication |
| `relation_configs/materialized_view.py` | MV configuration and changesets |
| `relation_configs/dist.py` | Distribution style configuration |
| `relation_configs/sort.py` | Sort key configuration |

## Available Hatch Scripts

| Command | Description |
|---------|-------------|
| `hatch run setup` | Initial development setup |
| `hatch run code-quality` | Run pre-commit on all files |
| `hatch run unit-tests` | Run unit tests |
| `hatch run integration-tests` | Run functional tests |
| `hatch run integration-tests-flaky` | Run flaky tests sequentially |
| `hatch run docker-dev` | Build and run dev Docker image |

## Dependencies

- **dbt-postgres** - Base SQL adapter functionality
- **redshift-connector** (>=2.1.8,<2.2) - Official Redshift Python driver
- **dbt-adapters**, **dbt-common**, **dbt-core** - Core dbt packages

## Best Practices

1. Test both unit and integration when modifying connection or SQL generation
2. Consider dist/sort key implications when modifying relation configs
3. Test IAM authentication paths when changing auth code
4. Validate materialized view changes against actual Redshift cluster
5. Keep redshift-connector version pinned due to API stability concerns
