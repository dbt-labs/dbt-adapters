# dbt-snowflake Adapter Development

Development guide specific to the dbt-snowflake adapter.

## Overview

dbt-snowflake provides Snowflake-specific functionality including dynamic tables, Iceberg tables, external catalog integrations, and multiple authentication methods.

## Adapter Architecture

```
dbt-snowflake/
├── src/dbt/
│   ├── adapters/snowflake/
│   │   ├── __init__.py              # Plugin registration
│   │   ├── __version__.py           # Version info
│   │   ├── connections.py           # SnowflakeConnectionManager, SnowflakeCredentials
│   │   ├── impl.py                  # SnowflakeAdapter
│   │   ├── relation.py              # SnowflakeRelation
│   │   ├── column.py                # SnowflakeColumn
│   │   ├── auth.py                  # Authentication methods
│   │   ├── constants.py             # Snowflake-specific constants
│   │   ├── adapter_response.py      # Custom response handling
│   │   ├── query_headers.py         # Custom query header format
│   │   ├── parse_model.py           # Model parsing logic
│   │   ├── catalogs/                # External catalog support
│   │   │   ├── iceberg_rest.py      # Iceberg REST catalog
│   │   │   ├── info_schema.py       # Information schema catalog
│   │   │   └── built_in.py          # Built-in catalog
│   │   ├── record/                  # Custom cursor handling
│   │   └── relation_configs/        # Dynamic table, etc.
│   └── include/snowflake/
│       └── macros/                  # Snowflake SQL implementations
├── tests/
│   ├── unit/                        # Unit tests
│   └── functional/                  # Integration tests
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

## Key Components

### SnowflakeAdapter (`impl.py`)

Extends `SQLAdapter` with Snowflake-specific features:

- **Constraint Support**: NOT NULL enforced, unique/primary key not enforced
- **Capabilities**: Schema metadata, table last modified, microbatch concurrency
- **Dynamic Tables**: Native support for Snowflake dynamic tables
- **Iceberg Tables**: Apache Iceberg table format support
- **Python Models**: Snowpark execution support

### SnowflakeConnectionManager (`connections.py`)

Uses `snowflake-connector-python` with support for:

- Username/password authentication
- Key pair authentication
- OAuth authentication
- SSO (externalbrowser) authentication
- Private link connections
- Query tagging and tracing

### SnowflakeCredentials (`connections.py`)

Key credential fields:

```python
account: str                    # Snowflake account identifier
user: str                       # Username
password: str                   # Password (optional with key pair)
database: str                   # Default database
warehouse: str                  # Default warehouse
role: str                       # User role
schema: str                     # Default schema
authenticator: str              # Auth method (snowflake, externalbrowser, oauth)
private_key_path: str           # Path to private key file
private_key_passphrase: str     # Private key passphrase
oauth_client_id: str            # OAuth client ID
oauth_client_secret: str        # OAuth client secret
client_session_keep_alive: bool # Keep session alive
query_tag: str                  # Query tagging for audit
```

### SnowflakeRelation (`relation.py`)

Supports relation-specific configurations:

- Dynamic tables with target lag
- Iceberg tables with external catalogs
- Transient/temporary tables
- Clustering keys

## Development Setup

### Initial Setup

```shell
cd dbt-snowflake
hatch run setup
```

### Environment Shell

```shell
hatch shell
```

## Test Environment Configuration

Create `test.env` from `test.env.example`:

### Basic Authentication

```shell
SNOWFLAKE_TEST_ACCOUNT=my_account_id
SNOWFLAKE_TEST_USER=my_username
SNOWFLAKE_TEST_PASSWORD=my_password
SNOWFLAKE_TEST_DATABASE=my_database_name
SNOWFLAKE_TEST_WAREHOUSE=my_warehouse_name
```

### Additional Test Resources

```shell
SNOWFLAKE_TEST_ALT_DATABASE=my_alt_database_name
SNOWFLAKE_TEST_ALT_WAREHOUSE=my_alt_warehouse_name
SNOWFLAKE_TEST_QUOTED_DATABASE=my_quoted_database_name
SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE=my_catalog_linked_database_name
```

### OAuth Authentication

```shell
SNOWFLAKE_TEST_OAUTH_CLIENT_ID=my_oauth_id
SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET=my_oauth_secret
SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN=TRUE
```

### Local Development Instance (Optional)

```shell
SNOWFLAKE_TEST_HOST=
SNOWFLAKE_TEST_PORT=
SNOWFLAKE_TEST_PROTOCOL=
```

### Test Roles

```shell
DBT_TEST_USER_1=dbt_test_role_1
DBT_TEST_USER_2=dbt_test_role_2
DBT_TEST_USER_3=dbt_test_role_3
```

## Running Tests

### Unit Tests

```shell
# All unit tests
hatch run unit-tests

# Specific file
hatch run unit-tests tests/unit/test_snowflake_adapter.py

# Specific test
hatch run unit-tests tests/unit/test_snowflake_adapter.py::TestClass::test_method
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

## Snowflake-Specific Features

### Dynamic Tables

```sql
{{ config(
    materialized='dynamic_table',
    target_lag='1 hour',
    snowflake_warehouse='my_warehouse',
) }}
```

### Iceberg Tables

```sql
{{ config(
    materialized='table',
    table_format='iceberg',
    external_volume='my_volume',
    base_location_subpath='path/to/data',
) }}
```

### Transient Tables

```sql
{{ config(
    materialized='table',
    transient=true,
) }}
```

### Clustering

```sql
{{ config(
    materialized='table',
    cluster_by=['date_column', 'category'],
) }}
```

### Query Tags

```sql
{{ config(
    query_tag='my_model_tag',
) }}
```

### Copy Grants

```sql
{{ config(
    materialized='table',
    copy_grants=true,
) }}
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with constraints, capabilities |
| `connections.py` | Connection management and credentials |
| `relation.py` | Relation handling with dynamic table configs |
| `column.py` | Snowflake-specific column types |
| `auth.py` | Authentication method implementations |
| `catalogs/` | External catalog integrations |
| `relation_configs/` | Dynamic table, Iceberg configurations |

## Available Hatch Scripts

| Command | Description |
|---------|-------------|
| `hatch run setup` | Initial development setup |
| `hatch run code-quality` | Run pre-commit on all files |
| `hatch run unit-tests` | Run unit tests |
| `hatch run integration-tests` | Run functional tests |

## Dependencies

- **snowflake-connector-python** - Official Snowflake Python driver
- **dbt-adapters**, **dbt-common**, **dbt-core** - Core dbt packages

## Best Practices

1. Test against actual Snowflake account for integration tests
2. Consider warehouse costs when running tests
3. Test dynamic table features with appropriate target lag
4. Validate Iceberg table configs with external catalog setup
5. Test multiple authentication methods when modifying auth code
