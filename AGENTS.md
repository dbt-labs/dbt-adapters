# AGENTS.md — dbt Adapters Monorepo

This file instructs AI coding agents on how to navigate, build, test, and contribute to this repository.

## Repository Layout

```
dbt-adapters/
├── dbt-adapters/          # Base framework (BaseAdapter, SQLAdapter, contracts, catalogs)
├── dbt-tests-adapter/     # Shared test suite for all adapters
├── dbt-postgres/          # PostgreSQL adapter
├── dbt-redshift/          # Redshift adapter (extends dbt-postgres)
├── dbt-snowflake/         # Snowflake adapter
├── dbt-bigquery/          # BigQuery adapter
├── dbt-spark/             # Spark / Databricks adapter
└── dbt-athena/            # AWS Athena adapter
```

Each adapter is an independent Python package with its own `pyproject.toml`, `hatch.toml`, and test suite.

## Environment Setup

All commands are run from within the specific adapter's directory, not the repo root.

```bash
cd dbt-{adapter}        # e.g., cd dbt-redshift
pip install hatch       # if not already installed
hatch run setup         # installs adapter + deps in editable mode
```

To configure the virtual environment for IDE use:
```bash
hatch config set dirs.env.virtual .hatch
```

## Building / Code Quality

```bash
hatch run code-quality  # runs Black (format), Flake8 (lint), MyPy (types)
```

Always run this before submitting changes. Fix all errors before committing.

## Testing

### Unit Tests (no database required)

```bash
hatch run unit-tests

# Run a specific test
hatch run unit-tests -- tests/unit/test_file.py::ClassName::test_method -v
```

Unit tests live in `tests/unit/`. They test Python logic without a live database.

### Integration Tests (requires live database)

```bash
hatch run integration-tests
```

Integration tests live in `tests/functional/`. They require a `test.env` file with credentials (never commit this file). See `test.env.example` for required variables.

For dbt-redshift only, flaky tests can be run sequentially:
```bash
hatch run integration-tests-flaky
```

### Test Fixture Pattern

Tests inherit from `dbt-tests-adapter` base classes:

```python
from dbt.tests.adapter.basic import BaseSimpleMaterializations

class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass
```

## Making Changes

### Where to Make Changes

- **SQL behavior changes**: edit macros in `src/dbt/include/{adapter}/macros/`
- **Python behavior changes**: edit `src/dbt/adapters/{adapter}/impl.py`
- **Connection/credential changes**: edit `src/dbt/adapters/{adapter}/connections.py`
- **Relation config changes**: edit `src/dbt/adapters/{adapter}/relation.py` or `relation_configs/`
- **Base framework changes**: make changes in `dbt-adapters/` and check impact on all adapters

### Macro Override Convention

Override default macros by prefixing with the adapter name:

```sql
-- src/dbt/include/{adapter}/macros/adapters.sql
{% macro {adapter}__list_relations_without_caching(schema_relation) %}
    -- adapter-specific SQL
{% endmacro %}
```

### Adding Adapter Methods Available in Macros

Use the `@available` decorator:

```python
from dbt.adapters.base.meta import available

class MyAdapter(SQLAdapter):
    @available
    def my_method(self):
        """Callable in Jinja as adapter.my_method()"""
        pass
```

### Declaring Capabilities

```python
from dbt.adapters.capability import Capability, CapabilitySupport, CapabilityDict, Support

class MyAdapter(SQLAdapter):
    _capabilities = CapabilityDict({
        Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
    })
```

## Changelog

Every user-facing change requires a changelog entry:

```bash
changie new
```

Categories: `Breaking Changes`, `Features`, `Fixes`, `Under the Hood`, `Dependencies`, `Security`

## Dependency Relationships

When modifying base packages, check downstream impact:

- Changes to `dbt-adapters` affect **all** adapters
- Changes to `dbt-postgres` affect **dbt-redshift**
- Changes to `dbt-tests-adapter` affect all adapter test suites

## External Catalog Integrations

The `dbt-adapters/src/dbt/adapters/catalogs/` package provides a plugin system for external table formats (Apache Iceberg, etc.):

- `CatalogIntegration` — abstract base; implement `build_relation(model)`
- `CatalogIntegrationClient` — registry; pass `SUPPORTED_CATALOGS` list in adapter `__init__`
- Validate required config in `__init__` and raise `InvalidCatalogIntegrationConfigError`

Existing implementations: Snowflake `iceberg_rest`, Snowflake `built_in`, BigQuery `biglake`.

## Security Rules

- Never commit `test.env` or any file containing credentials
- Never hardcode credentials, tokens, or access keys in source files
- Treat `test.env.example` as the authoritative list of required env vars (no values)

## Pull Request Checklist

- [ ] Code quality passes: `hatch run code-quality`
- [ ] Unit tests pass: `hatch run unit-tests`
- [ ] Integration tests pass against a real database (if changing SQL or connection logic)
- [ ] Changelog entry added via `changie new`
- [ ] `test.env` not committed
- [ ] New adapter methods decorated with `@available` if needed in macros
- [ ] Capabilities updated if new features are added
