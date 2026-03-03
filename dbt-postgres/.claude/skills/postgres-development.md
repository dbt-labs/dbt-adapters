# dbt-postgres Adapter Development

Development guide specific to the dbt-postgres adapter.

## Overview

dbt-postgres is the foundational SQL adapter that other adapters (like dbt-redshift) extend. It provides PostgreSQL-specific functionality including indexes, materialized views, and standard SQL patterns.

## Adapter Architecture

```
dbt-postgres/
├── src/dbt/
│   ├── adapters/postgres/
│   │   ├── __init__.py              # Plugin registration
│   │   ├── __version__.py           # Version info
│   │   ├── connections.py           # PostgresConnectionManager, PostgresCredentials
│   │   ├── impl.py                  # PostgresAdapter
│   │   ├── relation.py              # PostgresRelation
│   │   └── column.py                # PostgresColumn
│   └── include/postgres/
│       └── macros/                  # Postgres SQL implementations
├── tests/
│   ├── unit/                        # Unit tests
│   └── functional/                  # Integration tests
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

## Key Components

### PostgresAdapter (`impl.py`)

Base SQL adapter with PostgreSQL-specific features:

- **Constraint Support**: CHECK, NOT NULL, UNIQUE, PRIMARY KEY, FOREIGN KEY
- **Capabilities**: Schema metadata, catalog retrieval
- **Indexes**: Native index support on tables
- **Date Function**: `now()`

### PostgresConnectionManager (`connections.py`)

Uses `psycopg2` driver with support for:

- Standard PostgreSQL authentication
- SSL connections
- Connection pooling
- Search path configuration

### PostgresCredentials (`connections.py`)

Key credential fields:

```python
host: str                    # Database host
port: int = 5432             # Default PostgreSQL port
user: str                    # Username
password: str                # Password
database: str                # Database name
schema: str                  # Default schema
keepalives_idle: int         # TCP keepalive idle time
connect_timeout: int         # Connection timeout
search_path: str             # Schema search path
sslmode: str                 # SSL mode (disable, require, verify-ca, verify-full)
sslcert: str                 # Path to SSL certificate
sslkey: str                  # Path to SSL key
sslrootcert: str             # Path to SSL root certificate
```

### PostgresRelation (`relation.py`)

Standard relation handling with:

- Tables, views, materialized views
- Quote policy for identifiers
- Schema and database inclusion policy

## Development Setup

### Initial Setup

```shell
cd dbt-postgres
hatch run setup
```

### Environment Shell

```shell
hatch shell
```

## Test Environment Configuration

Create `test.env` from `test.env.example`:

```shell
POSTGRES_TEST_HOST=localhost
POSTGRES_TEST_PORT=5432
POSTGRES_TEST_USER=postgres
POSTGRES_TEST_PASS=postgres
POSTGRES_TEST_DATABASE=dbt_test
POSTGRES_TEST_THREADS=4
```

### Test Users

```shell
DBT_TEST_USER_1=dbt_test_user_1
DBT_TEST_USER_2=dbt_test_user_2
DBT_TEST_USER_3=dbt_test_user_3
```

### Local PostgreSQL Setup

For integration tests, you can use Docker:

```shell
docker run -d \
  --name postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=dbt_test \
  -p 5432:5432 \
  postgres:14
```

Create test users:

```sql
CREATE USER dbt_test_user_1 WITH PASSWORD 'password';
CREATE USER dbt_test_user_2 WITH PASSWORD 'password';
CREATE USER dbt_test_user_3 WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE dbt_test TO dbt_test_user_1;
GRANT ALL PRIVILEGES ON DATABASE dbt_test TO dbt_test_user_2;
GRANT ALL PRIVILEGES ON DATABASE dbt_test TO dbt_test_user_3;
```

## Running Tests

### Unit Tests

```shell
# All unit tests
hatch run unit-tests

# Specific file
hatch run unit-tests tests/unit/test_postgres_adapter.py

# Specific test
hatch run unit-tests tests/unit/test_postgres_adapter.py::TestClass::test_method
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

## PostgreSQL-Specific Features

### Indexes

```sql
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['column_a'], 'type': 'btree'},
      {'columns': ['column_b'], 'type': 'hash'},
      {'columns': ['column_a', 'column_b'], 'unique': true},
    ],
) }}
```

### Materialized Views

```sql
{{ config(
    materialized='materialized_view',
) }}
```

### Unlogged Tables

```sql
{{ config(
    materialized='table',
    unlogged=true,
) }}
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with constraints, index support |
| `connections.py` | Connection management with psycopg2 |
| `relation.py` | Standard SQL relation handling |
| `column.py` | PostgreSQL column types |

## Available Hatch Scripts

| Command | Description |
|---------|-------------|
| `hatch run setup` | Initial development setup |
| `hatch run code-quality` | Run pre-commit on all files |
| `hatch run unit-tests` | Run unit tests |
| `hatch run integration-tests` | Run functional tests |

## Dependencies

- **psycopg2-binary** or **psycopg2** - PostgreSQL Python driver
- **dbt-adapters**, **dbt-common**, **dbt-core** - Core dbt packages

## Importance for Other Adapters

dbt-postgres serves as the base for other adapters:

- **dbt-redshift** extends PostgresAdapter for Amazon Redshift
- Other adapters may reference Postgres patterns for SQL operations

When modifying dbt-postgres, consider impact on dependent adapters.

## Best Practices

1. Test with standard PostgreSQL before extending to other databases
2. Ensure backward compatibility for dependent adapters
3. Document any SQL dialect differences
4. Test index creation and maintenance operations
5. Validate SSL connection modes when modifying connection code
