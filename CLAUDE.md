# dbt Adapters — Development Guide

## Monorepo Structure

```
dbt-adapters/
├── dbt-adapters/          # Base framework and protocols
├── dbt-tests-adapter/     # Reusable test suite
├── dbt-postgres/          # PostgreSQL adapter (base for redshift)
├── dbt-redshift/          # Amazon Redshift adapter (extends postgres)
├── dbt-snowflake/         # Snowflake adapter
├── dbt-bigquery/          # Google BigQuery adapter
├── dbt-spark/             # Apache Spark / Databricks adapter
├── dbt-athena/            # AWS Athena adapter
└── .pre-commit-config.yaml
```

Dependency chain:
```
dbt-adapters (base)
├── dbt-postgres
│   └── dbt-redshift
├── dbt-snowflake
├── dbt-bigquery
├── dbt-spark
└── dbt-athena

dbt-tests-adapter → used by all adapters for testing
```

## Development Workflow

All commands run from the specific adapter directory (e.g. `cd dbt-redshift`).

```shell
# Prerequisites
pip install hatch changie pre-commit

# Initial setup (installs adapter + deps in editable mode)
hatch run setup

# Code quality (Black, Flake8, MyPy)
hatch run code-quality

# Unit tests (no database required)
hatch run unit-tests
hatch run unit-tests -- tests/unit/test_file.py::TestClass::test_method -v

# Integration tests (requires test.env)
hatch run integration-tests

# Changelog entry
changie new   # Categories: Breaking Changes, Features, Fixes, Under the Hood, Dependencies, Security

# IDE integration: use local .hatch virtualenv
hatch config set dirs.env.virtual .hatch
```

Never commit `test.env` credentials.

## Base Framework (`dbt-adapters`)

**Location:** `dbt-adapters/src/dbt/adapters/`

### Core Classes

| File | Class | Purpose |
|------|-------|---------|
| `base/impl.py` | `BaseAdapter` | Abstract base for all adapters |
| `base/connections.py` | `BaseConnectionManager` | Thread-safe connection pooling |
| `base/relation.py` | `BaseRelation` | Database relation representation |
| `base/column.py` | `Column` | Column metadata and type handling |
| `base/plugin.py` | `AdapterPlugin` | Plugin registration |
| `base/meta.py` | `@available` | Decorator for macro-accessible methods |
| `sql/impl.py` | `SQLAdapter` | SQL-specific base (extends BaseAdapter) |
| `sql/connections.py` | `SQLConnectionManager` | SQL connection patterns |

### Key Supporting Systems

- **`contracts/`** — `Credentials`, `Connection`, `RelationConfig` dataclasses
- **`capability.py`** — `Capability` enum and `CapabilityDict` for feature declaration
- **`cache.py`** — `RelationsCache` for performance optimization
- **`catalogs/`** — External catalog integrations (Iceberg, etc.)
- **`record/`** — Execution recording for replay/testing

### Global Macros (`include/global_project/macros/`)

- `materializations/` — table, view, incremental, snapshot, seed
- `relations/` — DDL for tables, views, columns
- `utils/` — Common utility macros

## Testing (`dbt-tests-adapter`)

**Location:** `dbt-tests-adapter/src/dbt/tests/adapter/`

### Pattern

```python
from dbt.tests.adapter.basic import BaseSimpleMaterializations

class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass  # inherits all base tests

class TestCustomFeature(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select 1 as id"}

    def test_custom_logic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
```

### Required Tests (`basic/`)

`BaseSimpleMaterializations`, `BaseEmpty`, `BaseEphemeral`, `BaseSnapshotTimestamp`, `BaseSnapshotCheck`, `BaseIncremental`, `BaseGenericTests`, `BaseSingularTests`, `BaseAdapterMethod`, `BaseValidateConnection`, `BaseDocsGenerate`

### Optional Tests

`incremental/`, `constraints/`, `grants/`, `materialized_view/`, `relations/`, `python_model/`, `unit_testing/`, `concurrency/`

## Adapter Structure

All adapters follow this layout:

```
dbt-{adapter}/
├── src/dbt/
│   ├── adapters/{adapter}/
│   │   ├── __init__.py           # Plugin registration
│   │   ├── connections.py        # ConnectionManager & Credentials
│   │   ├── impl.py               # Adapter implementation
│   │   ├── relation.py           # Relation class
│   │   └── column.py             # Column type handling (if needed)
│   └── include/{adapter}/macros/ # SQL macro overrides
├── tests/unit/
├── tests/functional/
├── pyproject.toml
├── hatch.toml
└── test.env.example
```

Macros override defaults by prefixing with adapter name:
```sql
{% macro redshift__list_relations_without_caching(schema_relation) %}
    -- Redshift-specific implementation
{% endmacro %}
```

---

## dbt-postgres

Foundation SQL adapter; other adapters extend it.

**Key files:** `impl.py`, `connections.py` (psycopg2), `relation.py`, `column.py`

**Capabilities:** All constraint types (CHECK, NOT NULL, UNIQUE, PRIMARY KEY, FOREIGN KEY), indexes, materialized views.

```sql
-- Indexes
{{ config(indexes=[
  {'columns': ['col_a'], 'type': 'btree'},
  {'columns': ['col_a', 'col_b'], 'unique': true},
]) }}

-- Materialized views
{{ config(materialized='materialized_view') }}

-- Unlogged tables
{{ config(materialized='table', unlogged=true) }}
```

**Credentials:** `host`, `port` (5432), `user`, `password`, `database`, `schema`, `sslmode`, `sslcert`, `sslkey`, `sslrootcert`, `search_path`

**Local test DB:**
```shell
docker run -d --name postgres-test -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=dbt_test -p 5432:5432 postgres:14
```

---

## dbt-redshift

Extends `PostgresAdapter`. Uses `redshift_connector` (>=2.1.8,<2.2).

**Key files:** `impl.py`, `connections.py`, `relation.py`, `auth_providers.py`, `relation_configs/` (materialized_view, dist, sort)

**Unique to Redshift:**

```sql
-- Distribution styles
{{ config(dist='key_column') }}   -- KEY
{{ config(dist='even') }}          -- EVEN
{{ config(dist='all') }}           -- ALL
{{ config(dist='auto') }}          -- AUTO

-- Sort keys
{{ config(sort=['col1', 'col2'], sort_type='interleaved') }}

-- Materialized views with auto-refresh
{{ config(materialized='materialized_view', auto_refresh=true, dist='col', sort=['col1']) }}

-- Late-binding views
{{ config(materialized='view', bind=false) }}
```

**Auth methods:** Basic (user/password), IAM user, IAM role, Identity Center (browser SSO)

**Credentials:** `host`, `port` (5439), `database`, `user`, `password`, `region`, `cluster_id`, `iam_profile`, `access_key_id`, `secret_access_key`, `autocommit`, `connect_timeout`

**Auth providers:** `IAMRoleAuthProvider`, `BrowserIdentityCenterAuthProvider`

**Datasharing:** The `datasharing` profile credential gates SHOW TABLES / SVV_* APIs vs legacy PostgreSQL catalog queries (required for cross-database operations). Jinja: `redshift__use_show_apis()`. Python: `adapter.use_show_apis()`.

**Extra test command:** `hatch run integration-tests-flaky` (runs flaky tests sequentially)

---

## dbt-snowflake

Extends `SQLAdapter`. Uses `snowflake-connector-python`.

**Key files:** `impl.py`, `connections.py`, `relation.py`, `auth.py`, `catalogs/` (iceberg_rest, built_in, info_schema), `relation_configs/`

**Unique to Snowflake:**

```sql
-- Dynamic tables
{{ config(materialized='dynamic_table', target_lag='1 hour', snowflake_warehouse='my_wh') }}

-- Iceberg tables
{{ config(materialized='table', table_format='iceberg', external_volume='my_vol', base_location_subpath='path/') }}

-- Transient tables
{{ config(materialized='table', transient=true) }}

-- Clustering
{{ config(cluster_by=['date_col', 'category']) }}

-- Copy grants
{{ config(materialized='table', copy_grants=true) }}

-- Query tags
{{ config(query_tag='my_tag') }}
```

**Auth methods:** Username/password, key pair, OAuth, SSO (externalbrowser), private link

**Credentials:** `account`, `user`, `password`, `database`, `warehouse`, `role`, `schema`, `authenticator`, `private_key_path`, `private_key_passphrase`, `oauth_client_id`, `oauth_client_secret`, `client_session_keep_alive`, `query_tag`

---

## dbt-bigquery

Extends `BaseAdapter` (not SQLAdapter). Uses `google-cloud-bigquery`.

**Key files:** `impl.py`, `connections.py`, `credentials.py`, `relation.py`, `column.py` (STRUCT support), `python_submissions.py`, `retry.py`, `catalogs/`

**Unique to BigQuery:**

```sql
-- Time partitioning
{{ config(partition_by={"field": "created_at", "data_type": "timestamp", "granularity": "day"}) }}

-- Integer range partitioning
{{ config(partition_by={"field": "user_id", "data_type": "int64", "range": {"start": 0, "end": 1000, "interval": 10}}) }}

-- Clustering
{{ config(cluster_by=['customer_id', 'order_date']) }}

-- Cost control
{{ config(maximum_bytes_billed=1000000000) }}

-- Labels
{{ config(labels={'team': 'analytics', 'env': 'prod'}) }}

-- Copy partitions (incremental)
{{ config(materialized='incremental', incremental_strategy='insert_overwrite',
          partition_by={'field': 'date_col', 'data_type': 'date'}, copy_partitions=true) }}

-- Iceberg
{{ config(materialized='table', table_format='iceberg') }}
```

**Auth methods:** Service account (keyfile or inline JSON), OAuth, Application Default Credentials, impersonation

**Credentials:** `project`, `dataset`, `location`, `keyfile`, `keyfile_json`, `method`, `impersonate_service_account`, `execution_project`, `maximum_bytes_billed`, `priority`, `timeout_seconds`

**Special types:** `BigQueryColumn` handles STRUCT, ARRAY, GEOGRAPHY, JSON

---

## dbt-spark

Extends `SQLAdapter`. Supports Databricks and open-source Spark.

**Key files:** `impl.py`, `connections.py`, `relation.py`, `column.py`, `session.py`, `python_submissions.py`

**Connection methods:** `thrift` (Spark Thrift Server), `odbc` (Databricks ODBC), `http` (Databricks HTTP endpoint), `databricks` (Databricks SQL connector)

**Unique to Spark:**

```sql
-- Delta Lake
{{ config(materialized='table', file_format='delta') }}

-- Partitioning
{{ config(materialized='table', partition_by=['year', 'month'], file_format='parquet') }}

-- Bucketing
{{ config(materialized='table', buckets=8, bucket_columns=['user_id']) }}

-- External location
{{ config(materialized='table', location='s3://bucket/path/') }}

-- Merge (incremental)
{{ config(materialized='incremental', file_format='delta', incremental_strategy='merge', unique_key='id') }}

-- Table properties
{{ config(tblproperties={'delta.autoOptimize.optimizeWrite': 'true'}) }}
```

**Credentials:** `method`, `host`, `port` (10001 for thrift), `token`, `cluster`, `http_path`, `endpoint`, `schema`, `connect_timeout`, `connect_retries`

**Dependencies:** `pyhive`, `thrift`, `databricks-sql-connector`, `pyodbc` (optional)

---

## dbt-athena

Extends `SQLAdapter`. Uses `pyathena` + `boto3`.

**Key files:** `impl.py`, `connections.py`, `relation.py`, `column.py`, `s3.py`, `lakeformation.py`, `python_submissions.py`

**Unique to Athena:**

```sql
-- External tables
{{ config(materialized='table', external_location='s3://bucket/path/', format='parquet') }}

-- Hive partitioning
{{ config(materialized='table', partitioned_by=['year', 'month'], format='parquet') }}

-- Iceberg tables
{{ config(materialized='table', table_type='iceberg', format='parquet') }}

-- Table properties
{{ config(tblproperties={'has_encrypted_data': 'false', 'classification': 'json'}) }}

-- Workgroup
{{ config(materialized='table', work_group='analytics_team') }}

-- Insert overwrite (incremental)
{{ config(materialized='incremental', incremental_strategy='insert_overwrite', partitioned_by=['date_col']) }}
```

**Credentials:** `s3_staging_dir`, `s3_tmp_table_dir`, `region_name`, `database`, `schema`, `work_group`, `aws_profile_name`, `aws_access_key_id`, `aws_secret_access_key`, `poll_interval`, `num_retries`, `threads`

---

## External Catalog Integrations

Enables adapters to work with Apache Iceberg and other external table formats.

**Location:** `dbt-adapters/src/dbt/adapters/catalogs/`

| Class | Purpose |
|-------|---------|
| `CatalogIntegration` | Abstract base for catalog implementations |
| `CatalogIntegrationConfig` | Protocol for user configuration |
| `CatalogRelation` | Protocol for catalog-specific relation data |
| `CatalogIntegrationClient` | Registry managing multiple integrations |

### Existing Implementations

| Adapter | Catalog Type | File |
|---------|--------------|------|
| Snowflake | `iceberg_rest` | `catalogs/_iceberg_rest.py` |
| Snowflake | `built_in` | `catalogs/_built_in.py` |
| BigQuery | `biglake` | `catalogs/_biglake_metastore.py` |

### Implementing a New Catalog

**1. CatalogRelation dataclass:**
```python
@dataclass
class MyCatalogRelation:
    catalog_type: str = "my_catalog_type"
    catalog_name: Optional[str] = None
    table_format: Optional[str] = "iceberg"
    external_volume: Optional[str] = None
```

**2. CatalogIntegration class:**
```python
from dbt.adapters.catalogs import CatalogIntegration, InvalidCatalogIntegrationConfigError

class MyCatalogIntegration(CatalogIntegration):
    catalog_type = "my_catalog_type"
    table_format = "iceberg"
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        self.external_volume = config.external_volume
        if not self.external_volume:
            raise InvalidCatalogIntegrationConfigError(config.name, "external_volume required")

    def build_relation(self, model: RelationConfig) -> MyCatalogRelation:
        return MyCatalogRelation(external_volume=self.external_volume)
```

**3. Register in adapter:**
```python
# catalogs/__init__.py
SUPPORTED_CATALOGS = [MyCatalogIntegration]

# impl.py
self._catalog_client = CatalogIntegrationClient(SUPPORTED_CATALOGS)
```

**4. Use in macros:**
```sql
{% macro {adapter}__create_table_as(temporary, relation, sql) %}
    {%- set catalog_relation = adapter.get_catalog_relation(config.model) -%}
    {% if catalog_relation and catalog_relation.table_format == 'iceberg' %}
        {{ {adapter}__create_iceberg_table(relation, sql, catalog_relation) }}
    {% else %}
        {{ default__create_table_as(temporary, relation, sql) }}
    {% endif %}
{% endmacro %}
```

**User config (`dbt_project.yml`):**
```yaml
catalogs:
  - name: my_iceberg_catalog
    active_write_integration: my_write_integration
    write_integrations:
      - name: my_write_integration
        catalog_type: my_catalog_type
        table_format: iceberg
        external_volume: "my_volume"
```

---

## Creating a New Adapter

1. Choose base class: `SQLAdapter` for SQL databases, `BaseAdapter` for others
2. Implement required methods: `ConnectionManager.exception_handler()`, `.open()`, `.cancel()`, `Adapter.date_function()`
3. Define `Credentials` dataclass with connection parameters
4. Create `Relation` subclass if database has specific features
5. Register plugin in `__init__.py`:
   ```python
   Plugin = AdapterPlugin(adapter=MyAdapter, credentials=MyCredentials, include_path=..., dependencies=["postgres"])
   ```
6. Implement macro overrides in `include/{adapter}/macros/`
7. Add tests inheriting from `dbt-tests-adapter` base classes
8. Declare `CONSTRAINT_SUPPORT` dict
9. Declare `_capabilities` via `CapabilityDict`

---

## Best Practices

- Always work from the specific adapter directory, not the repo root
- Run `hatch run code-quality` before committing
- Use `@available` decorator for adapter methods needed in Jinja macros
- Declare capabilities accurately — dbt-core uses them for optimization
- Write both unit and integration tests for new functionality
- For catalog integrations: validate required config in `__init__` and raise `InvalidCatalogIntegrationConfigError` with clear messages
- Never commit `test.env` credentials
- Create `changie new` entries for all user-facing changes
- When modifying `dbt-postgres`, consider impact on `dbt-redshift`
