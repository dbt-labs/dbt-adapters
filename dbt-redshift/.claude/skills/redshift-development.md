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
│   │   ├── impl.py                  # RedshiftAdapter with CATALOG_INTEGRATIONS
│   │   ├── relation.py              # RedshiftRelation
│   │   ├── auth_providers.py        # IAM, Identity Center auth
│   │   ├── utility.py               # Helper functions
│   │   ├── constants.py             # Catalog type constants
│   │   ├── parse_model.py           # Model config parsing helpers
│   │   ├── catalogs/                # Catalog integrations
│   │   │   ├── __init__.py
│   │   │   ├── _glue.py             # GlueCatalogIntegration for Iceberg
│   │   │   ├── _info_schema.py      # Default Redshift catalog
│   │   │   └── _relation.py         # RedshiftCatalogRelation dataclass
│   │   └── relation_configs/        # Materialized view, dist, sort configs
│   │       ├── base.py
│   │       ├── materialized_view.py
│   │       ├── dist.py
│   │       ├── sort.py
│   │       └── policies.py
│   └── include/redshift/
│       └── macros/                  # Redshift SQL implementations
│           └── relations/iceberg/   # Iceberg table creation macros
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
- **Catalog Integrations**: GlueCatalogIntegration, RedshiftInfoSchemaCatalogIntegration

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

### Iceberg/Glue Catalog Testing

```shell
REDSHIFT_ICEBERG_EXTERNAL_SCHEMA=dbt_iceberg_schema
REDSHIFT_ICEBERG_S3_BUCKET=s3://your-dbt-iceberg-test-bucket/iceberg-data
REDSHIFT_ICEBERG_IAM_ROLE=arn:aws:iam::YOUR_ACCOUNT_ID:role/RedshiftGlueIcebergRole
REDSHIFT_ICEBERG_GLUE_DATABASE=dbt_iceberg_test_db
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

## Iceberg Tables via AWS Glue Data Catalog

dbt-redshift supports creating Iceberg tables managed by AWS Glue Data Catalog. This enables open table format storage with features like time travel, schema evolution, and cross-engine compatibility.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        dbt Model Config                         │
│              catalog='my_glue_catalog'                          │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   GlueCatalogIntegration                        │
│    - Reads catalog config from dbt_project.yml                  │
│    - Builds RedshiftCatalogRelation with storage_uri, etc.      │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Redshift External Schema                       │
│    - Points to Glue Data Catalog database                       │
│    - Requires IAM role with Glue/S3 permissions                 │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                AWS Glue Data Catalog                            │
│    - Stores Iceberg table metadata                              │
│    - Table definitions, schemas, partition info                 │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Amazon S3                                  │
│    - Stores Iceberg data files (Parquet)                        │
│    - Stores Iceberg metadata files                              │
│    - Path: {external_volume}/{schema}/{model_name}/             │
└─────────────────────────────────────────────────────────────────┘
```

### Prerequisites

1. **External Schema**: Must be created in Redshift before running dbt:

```sql
CREATE EXTERNAL SCHEMA dbt_iceberg_schema
FROM DATA CATALOG
DATABASE 'my_glue_database'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftGlueRole'
REGION 'us-east-1';
```

2. **IAM Role**: Redshift cluster needs an IAM role with permissions for:
   - AWS Glue Data Catalog (read/write tables, databases)
   - Amazon S3 (read/write to data bucket)

3. **S3 Bucket**: For storing Iceberg table data and metadata

### Configuration

#### Project-Level Catalog Configuration (`dbt_project.yml`)

```yaml
catalogs:
  - name: my_glue_catalog
    active_write_integration: glue_integration
    write_integrations:
      - name: glue_integration
        catalog_type: glue
        table_format: iceberg
        file_format: parquet
        external_volume: s3://my-bucket/iceberg-data
        adapter_properties:
          external_schema: dbt_iceberg_schema
          glue_database: my_glue_database  # Optional
```

#### Model Configuration

```sql
{{ config(
    materialized='table',
    catalog='my_glue_catalog',
    partition_by=['date_column', 'region']  -- Optional partitioning
) }}

SELECT * FROM {{ ref('source_table') }}
```

#### Model-Level Overrides

```sql
{{ config(
    materialized='table',
    catalog='my_glue_catalog',
    external_schema='custom_schema',           -- Override catalog default
    storage_uri='s3://custom/path/my_table/'   -- Override auto-generated path
) }}
```

### Key Components

#### `constants.py`

```python
ICEBERG_TABLE_FORMAT = "iceberg"
DEFAULT_TABLE_FORMAT = "default"
PARQUET_FILE_FORMAT = "parquet"
GLUE_CATALOG_TYPE = "glue"
INFO_SCHEMA_CATALOG_TYPE = "INFO_SCHEMA"
```

#### `GlueCatalogIntegration` (`catalogs/_glue.py`)

- Extends `CatalogIntegration` base class from dbt-adapters
- Parses `adapter_properties` for `external_schema` and `glue_database`
- Auto-generates storage URI: `{external_volume}/{schema}/{model_name}/`
- Builds `RedshiftCatalogRelation` with all Iceberg configuration

#### `RedshiftCatalogRelation` (`catalogs/_relation.py`)

```python
@dataclass
class RedshiftCatalogRelation:
    catalog_type: str           # "glue" or "INFO_SCHEMA"
    catalog_name: Optional[str]
    table_format: Optional[str] # "iceberg" or "default"
    file_format: Optional[str]  # "parquet"
    external_volume: Optional[str]
    storage_uri: Optional[str]  # Full S3 path for data
    glue_database: Optional[str]
    external_schema: Optional[str]
    partition_by: Optional[List[str]]
```

#### `parse_model.py`

Helper functions to extract catalog config from model:

- `catalog_name(model)` - Get catalog name from config
- `external_schema(model)` - Get external schema override
- `storage_uri(model)` - Get explicit storage URI
- `partition_by(model)` - Get partition columns (normalizes string to list)
- `file_format(model)` - Get file format override

### Macro Flow

1. `redshift__create_table_as` checks if model has Iceberg catalog
2. Routes to `redshift__create_iceberg_table_as` for Iceberg tables
3. Creates table in external schema with Iceberg properties
4. Inserts data from model SQL

### Type Mapping (Redshift → Iceberg)

| Redshift Type | Iceberg Type |
|---------------|--------------|
| SMALLINT, INT2 | INT |
| INTEGER, INT, INT4 | INT |
| BIGINT, INT8 | BIGINT |
| DECIMAL, NUMERIC | DECIMAL |
| REAL, FLOAT4 | FLOAT |
| DOUBLE PRECISION, FLOAT8 | DOUBLE |
| BOOLEAN, BOOL | BOOLEAN |
| CHAR, CHARACTER, NCHAR, BPCHAR | STRING |
| VARCHAR, NVARCHAR, TEXT | STRING |
| DATE | DATE |
| TIMESTAMP, TIMESTAMP WITHOUT TIME ZONE | TIMESTAMP |
| TIMESTAMPTZ, TIMESTAMP WITH TIME ZONE | TIMESTAMPTZ |
| TIME, TIME WITHOUT TIME ZONE | STRING |
| TIMETZ | STRING |
| VARBYTE, BINARY, VARBINARY | BINARY |
| SUPER | STRING |
| HLLSKETCH | BINARY |
| GEOMETRY, GEOGRAPHY | STRING |

### Testing Iceberg Support

#### Unit Tests

```shell
hatch run unit-tests tests/unit/test_glue_catalog_integration.py -v
```

#### Functional Tests (requires AWS setup)

```shell
# Set environment variables first
export REDSHIFT_ICEBERG_EXTERNAL_SCHEMA=dbt_iceberg_schema
export REDSHIFT_ICEBERG_S3_BUCKET=s3://my-bucket/iceberg

hatch run integration-tests tests/functional/adapter/iceberg/
```

### AWS Setup Commands

#### Create IAM Role

```bash
# Create trust policy
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create the IAM role
aws iam create-role \
  --role-name RedshiftGlueIcebergRole \
  --assume-role-policy-document file://trust-policy.json

# Create permissions policy
cat > permissions-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartitions",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/*",
        "arn:aws:glue:*:*:table/*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::your-iceberg-bucket",
        "arn:aws:s3:::your-iceberg-bucket/*"
      ]
    }
  ]
}
EOF

# Attach the policy
aws iam put-role-policy \
  --role-name RedshiftGlueIcebergRole \
  --policy-name GlueS3IcebergAccess \
  --policy-document file://permissions-policy.json
```

#### Create External Schema in Redshift

```sql
-- First, create a Glue database (if needed)
-- Can be done via AWS Console or Glue API

-- Then create external schema in Redshift
CREATE EXTERNAL SCHEMA dbt_iceberg_schema
FROM DATA CATALOG
DATABASE 'dbt_iceberg_test_db'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/RedshiftGlueIcebergRole'
REGION 'us-east-1';
```

### Limitations

- **Table materialization only**: Incremental models not yet supported
- **External schema required**: Must be pre-created in Redshift
- **Partitions are identity transforms**: No complex partition transforms (bucket, truncate, etc.)
- **No MERGE support**: Full table replacement on each run

## Key Files Reference

| File | Purpose |
|------|---------|
| `impl.py` | Main adapter with constraints, capabilities, type conversions, catalog integrations |
| `connections.py` | Connection management and credentials |
| `relation.py` | Relation handling with dist/sort configs |
| `auth_providers.py` | IAM and Identity Center authentication |
| `constants.py` | Catalog type constants (GLUE, INFO_SCHEMA, ICEBERG) |
| `parse_model.py` | Model config parsing for catalog settings |
| `catalogs/_glue.py` | GlueCatalogIntegration for Iceberg tables |
| `catalogs/_info_schema.py` | Default catalog for standard Redshift tables |
| `catalogs/_relation.py` | RedshiftCatalogRelation dataclass |
| `relation_configs/materialized_view.py` | MV configuration and changesets |
| `relation_configs/dist.py` | Distribution style configuration |
| `relation_configs/sort.py` | Sort key configuration |
| `macros/relations/iceberg/create.sql` | Iceberg table creation macro |

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
6. When adding catalog integrations, follow the pattern from dbt-snowflake/dbt-bigquery
7. For Iceberg tables, ensure external schema exists before running dbt models
8. Use unit tests with MagicMock for catalog integration testing - use real dicts for config
