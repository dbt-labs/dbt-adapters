# dbt Adapter Development Skill

This skill guides development of dbt adapters using hatch, changie, and pre-commit in the dbt-adapters monorepo.

## Monorepo Structure

```
dbt-adapters/
├── dbt-adapters/          # Base framework and protocols
├── dbt-tests-adapter/     # Reusable test suite
├── dbt-postgres/          # PostgreSQL adapter
├── dbt-redshift/          # Amazon Redshift adapter (extends postgres)
├── dbt-snowflake/         # Snowflake adapter
├── dbt-bigquery/          # Google BigQuery adapter
├── dbt-spark/             # Apache Spark adapter
├── dbt-athena/            # AWS Athena adapter
└── .pre-commit-config.yaml
```

## dbt-adapters Package (Base Framework)

**Location:** `dbt-adapters/src/dbt/adapters/`

### Core Base Classes (`base/`)

| File | Class | Purpose |
|------|-------|---------|
| `impl.py` | `BaseAdapter` | Abstract base for all adapters |
| `impl.py` | `AdapterConfig` | Configuration dataclass |
| `connections.py` | `BaseConnectionManager` | Thread-safe connection pooling |
| `relation.py` | `BaseRelation` | Database relation representation |
| `column.py` | `Column` | Column metadata and type handling |
| `plugin.py` | `AdapterPlugin` | Plugin registration |
| `meta.py` | `@available` | Decorator for macro-accessible methods |

### SQL Adapter Layer (`sql/`)

| File | Class | Purpose |
|------|-------|---------|
| `impl.py` | `SQLAdapter` | SQL-specific base (extends BaseAdapter) |
| `connections.py` | `SQLConnectionManager` | SQL connection patterns |

### Key Supporting Systems

- **`contracts/`** - `Credentials`, `Connection`, `RelationConfig` dataclasses
- **`capability.py`** - `Capability` enum and `CapabilityDict` for feature declaration
- **`cache.py`** - `RelationsCache` for performance optimization
- **`exceptions/`** - Domain-specific exceptions
- **`record/`** - Execution recording for replay/testing
- **`events/`** - Adapter-aware logging
- **`catalogs/`** - External catalog integrations (Iceberg, etc.)

### Global Project Macros (`include/global_project/macros/`)

- `materializations/` - table, view, incremental, snapshot, seed implementations
- `relations/` - DDL operations for tables, views, columns
- `utils/` - Common utility macros
- `generic_test_sql/` - Test SQL templates

## dbt-tests-adapter Package

**Location:** `dbt-tests-adapter/src/dbt/tests/adapter/`

Provides reusable test classes that adapters inherit from to validate behavior.

### Required Tests (`basic/`)

| Test Class | Purpose |
|------------|---------|
| `BaseSimpleMaterializations` | Table/view creation |
| `BaseEmpty` | Empty relation handling |
| `BaseEphemeral` | Ephemeral model support |
| `BaseSnapshotTimestamp` | Timestamp-based snapshots |
| `BaseSnapshotCheck` | Check column snapshots |
| `BaseIncremental` | Incremental model basics |
| `BaseGenericTests` | Generic test execution |
| `BaseSingularTests` | Singular test execution |
| `BaseAdapterMethod` | Adapter method validation |
| `BaseValidateConnection` | Connection validation |
| `BaseDocsGenerate` | Documentation generation |

### Optional Tests (Feature-Specific)

| Directory | Tests |
|-----------|-------|
| `incremental/` | Unique ID, predicates, schema change, microbatch |
| `constraints/` | Constraint support testing |
| `grants/` | Model, snapshot, seed, incremental grants |
| `materialized_view/` | MV creation and changes |
| `relations/` | Schema dropping, relation type changes |
| `python_model/` | Python model execution |
| `unit_testing/` | Unit test support |
| `concurrency/` | Concurrent execution |

### Test Fixture Pattern

```python
from dbt.tests.adapter.basic import BaseSimpleMaterializations

class TestSimpleMaterializations(BaseSimpleMaterializations):
    """Inherit all base tests automatically"""
    pass

class TestCustomFeature(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select 1 as id"}

    def test_custom_logic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
```

## Implementing Adapter Structure

All adapters follow a consistent structure:

```
dbt-{adapter}/
├── src/dbt/
│   ├── adapters/{adapter}/
│   │   ├── __init__.py           # Plugin registration
│   │   ├── __version__.py        # Version info
│   │   ├── connections.py        # ConnectionManager & Credentials
│   │   ├── impl.py               # Main adapter implementation
│   │   ├── relation.py           # Relation class
│   │   ├── column.py             # Column type handling (optional)
│   │   └── relation_configs/     # Relation-specific configs (optional)
│   └── include/{adapter}/
│       └── macros/               # SQL macro implementations
├── tests/
│   ├── unit/                     # Unit tests (no DB required)
│   └── functional/               # Integration tests (DB required)
├── pyproject.toml
├── hatch.toml
├── .changie.yaml
└── test.env.example
```

### Key Implementation Files

**`__init__.py` - Plugin Registration**
```python
from dbt.adapters.base import AdapterPlugin
from dbt.adapters.{adapter}.connections import {Adapter}ConnectionManager, {Adapter}Credentials
from dbt.adapters.{adapter}.impl import {Adapter}Adapter

Plugin = AdapterPlugin(
    adapter={Adapter}Adapter,
    credentials={Adapter}Credentials,
    include_path={adapter}.PACKAGE_PATH,
    dependencies=["postgres"],  # Optional
)
```

**`connections.py` - Connection Management**
```python
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import Credentials

@dataclass
class {Adapter}Credentials(Credentials):
    host: str
    port: int = 5439
    # ... database-specific fields

class {Adapter}ConnectionManager(SQLConnectionManager):
    TYPE = "{adapter}"

    @contextmanager
    def exception_handler(self, sql):
        # Handle database-specific exceptions

    def open(cls, connection):
        # Open database connection

    def cancel(self, connection):
        # Cancel running query
```

**`impl.py` - Adapter Implementation**
```python
from dbt.adapters.sql import SQLAdapter

class {Adapter}Adapter(SQLAdapter):
    Relation = {Adapter}Relation
    ConnectionManager = {Adapter}ConnectionManager
    Column = {Adapter}Column

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        # ...
    }

    _capabilities = CapabilityDict({
        Capability.SchemaMetadataByRelations: CapabilitySupport.FULL,
        # ...
    })

    @classmethod
    def date_function(cls):
        return "GETDATE()"

    @available
    def custom_method(self):
        """Available in macros via adapter.custom_method()"""
        pass
```

**`relation.py` - Relation Handling**
```python
from dbt.adapters.base.relation import BaseRelation

@dataclass(frozen=True, eq=False, repr=False)
class {Adapter}Relation(BaseRelation):
    quote_policy = {Adapter}QuotePolicy
    include_policy = {Adapter}IncludePolicy

    relation_configs = {
        RelationType.MaterializedView.value: {Adapter}MaterializedViewConfig,
    }
```

### Macro Override Pattern

Adapters override default macros by prefixing with adapter name:

```sql
-- include/{adapter}/macros/adapters.sql
{% macro {adapter}__list_relations_without_caching(schema_relation) %}
    -- Database-specific implementation
{% endmacro %}

{% macro {adapter}__get_columns_in_relation(relation) %}
    -- Database-specific implementation
{% endmacro %}
```

## Dependency Relationships

```
dbt-adapters (base)
├── dbt-postgres (standalone SQL adapter)
│   └── dbt-redshift (extends postgres)
├── dbt-snowflake (custom SQL adapter)
├── dbt-bigquery (custom non-SQL backend)
├── dbt-spark (custom Spark implementation)
└── dbt-athena (custom Athena implementation)

dbt-tests-adapter → used by all adapters for testing
```

## Development Workflow

### Prerequisites

```shell
pip install hatch changie pre-commit
```

### Initial Setup (from adapter directory)

```shell
cd dbt-{adapter}
hatch run setup
```

### Configure Hatch for IDE Integration

```shell
hatch config set dirs.env.virtual .hatch
```

### Code Quality

```shell
hatch run code-quality
```

Runs: Black (formatting), Flake8 (linting), MyPy (type checking)

### Testing

```shell
# Unit tests (no database)
hatch run unit-tests

# Integration tests (requires test.env)
hatch run integration-tests
```

### Changelog Entry

```shell
changie new
```

Categories: Breaking Changes, Features, Fixes, Under the Hood, Dependencies, Security

### Commit and Push

```shell
git add .
git commit -m "feat: description"
git push origin feature/branch
```

## Creating a New Adapter

1. **Choose base class**: `SQLAdapter` for SQL databases, `BaseAdapter` for others
2. **Implement required methods**:
   - `ConnectionManager.exception_handler()`
   - `ConnectionManager.open()`
   - `ConnectionManager.cancel()`
   - `Adapter.date_function()` (for SQL adapters)
3. **Define credentials dataclass** with connection parameters
4. **Create relation class** if database has specific features
5. **Register plugin** in `__init__.py`
6. **Implement macros** in `include/{adapter}/macros/`
7. **Add tests** inheriting from `dbt-tests-adapter` base classes
8. **Document constraints** via `CONSTRAINT_SUPPORT`
9. **Declare capabilities** via `_capabilities`

## Developing External Catalog Integrations

External catalog integrations enable adapters to work with external table formats like Apache Iceberg, using external catalog systems (Iceberg REST, Glue, BigLake, etc.).

### Catalog System Architecture

**Location:** `dbt-adapters/src/dbt/adapters/catalogs/`

| File | Class | Purpose |
|------|-------|---------|
| `_integration.py` | `CatalogIntegration` | Abstract base for catalog implementations |
| `_integration.py` | `CatalogIntegrationConfig` | Protocol for user configuration |
| `_integration.py` | `CatalogRelation` | Protocol for catalog-specific relation data |
| `_client.py` | `CatalogIntegrationClient` | Registry managing catalog integrations |
| `_exceptions.py` | Various | Catalog-specific exceptions |

### CatalogIntegrationConfig Protocol

User configuration provided in `dbt_project.yml`:

```python
class CatalogIntegrationConfig(Protocol):
    name: str                           # Unique name for this integration
    catalog_type: str                   # Type (e.g., "iceberg_rest", "biglake")
    catalog_name: Optional[str]         # Name in the data platform
    table_format: Optional[str]         # Table format (e.g., "iceberg")
    external_volume: Optional[str]      # Storage volume identifier
    file_format: Optional[str]          # File format (e.g., "parquet")
    adapter_properties: Dict[str, Any]  # Adapter-specific properties
```

### Implementing a Catalog Integration

**Step 1: Create the CatalogRelation dataclass**

```python
# catalogs/_my_catalog.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class MyCatalogRelation:
    catalog_type: str = "my_catalog_type"
    catalog_name: Optional[str] = None
    table_format: Optional[str] = "iceberg"
    external_volume: Optional[str] = None
    file_format: Optional[str] = None
    # Add catalog-specific fields
    storage_uri: Optional[str] = None
    partition_by: Optional[list[str]] = None
```

**Step 2: Implement the CatalogIntegration class**

```python
from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
    InvalidCatalogIntegrationConfigError,
)
from dbt.adapters.contracts.relation import RelationConfig

class MyCatalogIntegration(CatalogIntegration):
    catalog_type = "my_catalog_type"  # Must match user config
    table_format = "iceberg"          # Default table format
    allows_writes = True              # Can this catalog be written to?

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        self.name = config.name
        self.catalog_name = config.catalog_name
        self.external_volume = config.external_volume

        # Parse adapter-specific properties
        if adapter_properties := config.adapter_properties:
            self.custom_setting = adapter_properties.get("custom_setting")

        # Validate required configuration
        if not self.external_volume:
            raise InvalidCatalogIntegrationConfigError(
                config.name,
                "external_volume is required for this catalog type"
            )

    def build_relation(self, model: RelationConfig) -> MyCatalogRelation:
        """Build relation config from model configuration."""
        return MyCatalogRelation(
            catalog_name=self.catalog_name,
            external_volume=self.external_volume,
            storage_uri=self._calculate_storage_uri(model),
            partition_by=model.config.get("partition_by") if model.config else None,
        )

    def _calculate_storage_uri(self, model: RelationConfig) -> Optional[str]:
        """Calculate storage URI from model config or defaults."""
        if not model.config:
            return None
        if model_uri := model.config.get("storage_uri"):
            return model_uri
        # Build default URI from external volume and model info
        return f"{self.external_volume}/{model.schema}/{model.name}"
```

**Step 3: Register the catalog in the adapter**

```python
# catalogs/__init__.py
from dbt.adapters.{adapter}.catalogs._my_catalog import MyCatalogIntegration

SUPPORTED_CATALOGS = [MyCatalogIntegration]
```

```python
# impl.py
from dbt.adapters.catalogs import CatalogIntegrationClient
from dbt.adapters.{adapter}.catalogs import SUPPORTED_CATALOGS

class MyAdapter(SQLAdapter):
    def __init__(self, config, mp_context):
        super().__init__(config, mp_context)
        self._catalog_client = CatalogIntegrationClient(SUPPORTED_CATALOGS)
```

**Step 4: Update macros to use catalog relation**

```sql
-- macros/relations/table/create.sql
{% macro {adapter}__create_table_as(temporary, relation, sql) %}
    {%- set catalog_relation = adapter.get_catalog_relation(config.model) -%}

    {% if catalog_relation and catalog_relation.table_format == 'iceberg' %}
        {{ {adapter}__create_iceberg_table(relation, sql, catalog_relation) }}
    {% else %}
        {{ default__create_table_as(temporary, relation, sql) }}
    {% endif %}
{% endmacro %}

{% macro {adapter}__create_iceberg_table(relation, sql, catalog_relation) %}
    create iceberg table {{ relation }}
    {% if catalog_relation.external_volume %}
        external_volume = '{{ catalog_relation.external_volume }}'
    {% endif %}
    {% if catalog_relation.partition_by %}
        partition by ({{ catalog_relation.partition_by | join(', ') }})
    {% endif %}
    as (
        {{ sql }}
    )
{% endmacro %}
```

### User Configuration Example

```yaml
# dbt_project.yml
catalogs:
  - name: my_iceberg_catalog
    active_write_integration: my_write_integration
    write_integrations:
      - name: my_write_integration
        catalog_type: my_catalog_type
        table_format: iceberg
        external_volume: "my_external_volume"
        adapter_properties:
          custom_setting: "value"
```

```sql
-- models/my_model.sql
{{ config(
    materialized='table',
    catalog='my_iceberg_catalog',
    partition_by=['date_column'],
) }}
select * from source_table
```

### Existing Catalog Implementations

| Adapter | Catalog Type | File | Features |
|---------|--------------|------|----------|
| Snowflake | `iceberg_rest` | `catalogs/_iceberg_rest.py` | Catalog-linked databases, partition_by |
| Snowflake | `built_in` | `catalogs/_built_in.py` | Native Snowflake Iceberg |
| BigQuery | `biglake` | `catalogs/_biglake_metastore.py` | BigLake managed tables |

### Testing Catalog Integrations

**Unit tests** - Test configuration parsing and relation building:

```python
def test_catalog_relation_building():
    config = MockCatalogIntegrationConfig(
        name="test_catalog",
        catalog_type="my_catalog_type",
        external_volume="gs://bucket",
    )
    integration = MyCatalogIntegration(config)

    model = MockRelationConfig(schema="my_schema", name="my_model")
    relation = integration.build_relation(model)

    assert relation.storage_uri == "gs://bucket/my_schema/my_model"
```

**Functional tests** - Test actual table creation:

```python
class TestMyCatalogIntegration:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "catalogs": [{
                "name": "test_catalog",
                "active_write_integration": "test_integration",
                "write_integrations": [{
                    "name": "test_integration",
                    "catalog_type": "my_catalog_type",
                    "external_volume": os.environ["TEST_EXTERNAL_VOLUME"],
                }],
            }]
        }

    def test_create_iceberg_table(self, project):
        results = run_dbt(["run", "-s", "iceberg_model"])
        assert len(results) == 1
```

### Reference PRs

- [PR #1251](https://github.com/dbt-labs/dbt-adapters/pull/1251) - Snowflake Iceberg REST with catalog-linked databases
- [PR #1430](https://github.com/dbt-labs/dbt-adapters/pull/1430) - Snowflake partition_by for Iceberg
- [PR #1105](https://github.com/dbt-labs/dbt-adapters/pull/1105) - BigQuery Iceberg catalog support

## Best Practices

1. Always work from the specific adapter directory
2. Run `hatch run code-quality` before committing
3. Write tests for new functionality
4. Use `@available` decorator for methods needed in macros
5. Declare capabilities accurately for dbt-core optimization
6. Never commit `test.env` credentials
7. Create changelog entries for user-facing changes
8. For catalog integrations, validate required config in `__init__` and provide clear error messages
