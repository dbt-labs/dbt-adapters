# Implementing External Data Catalog Support in dbt Core Adapters

This guide provides step-by-step instructions for dbt Core adapter maintainers to implement support for external data catalogs (such as Iceberg, Glue, Unity Catalog, etc.) in their adapters.

## Overview

External data catalog support, introduced in dbt Core v1.10, allows users to materialize dbt models into external catalogs that operate above specific table formats like Iceberg. This provides a warehouse-agnostic interface for managing datasets in object storage.

As detailed in the [dbt Core catalog integration discussion](https://github.com/dbt-labs/dbt-core/discussions/11171), this feature centralizes catalog configuration and abstracts differences between catalog providers.

## 1. How the `catalogs.yml` User Configuration Works

### Basic Structure

Users create a `catalogs.yml` file in their dbt project root with the following structure:

```yaml
catalogs:
  - name: my_iceberg_catalog
    active_write_integration: iceberg_integration
    write_integrations:
      - name: iceberg_integration
        catalog_type: iceberg_rest
        catalog_name: my_catalog_in_platform
        table_format: iceberg
        external_volume: s3_bucket_volume
        file_format: parquet
        adapter_properties:
          storage_uri: s3://my-bucket/path/
          custom_property: value
```

### Configuration Fields

- **`name`**: Unique identifier for the catalog integration in the dbt project
- **`active_write_integration`**: Which write integration to use (defaults to first if only one exists)
- **`write_integrations`**: List of integration configurations
  - **`catalog_type`**: Type of catalog (e.g., "iceberg_rest", "unity", "glue")
  - **`catalog_name`**: Name of the catalog in the data platform
  - **`table_format`**: Table format (e.g., "iceberg", "delta")
  - **`external_volume`**: External storage volume identifier
  - **`file_format`**: File format (e.g., "parquet", "delta")
  - **`adapter_properties`**: Adapter-specific properties

### Usage in Models

Users reference catalogs in their model configurations:

```sql
{{
  config(
    materialized='table',
    catalog_name='my_iceberg_catalog'
  )
}}

select * from my_table
```

## 2. What dbt-core Passes to the Adapter

### CatalogIntegrationConfig Protocol

dbt-core passes a `CatalogIntegrationConfig` object to your adapter's `add_catalog_integration` method. This object contains:

```python
class CatalogIntegrationConfig(Protocol):
    name: str
    catalog_type: str
    catalog_name: Optional[str]
    table_format: Optional[str]
    external_volume: Optional[str]
    file_format: Optional[str]
    adapter_properties: Dict[str, Any]
```

### Integration Loading Process

1. dbt-core loads `catalogs.yml` during startup
2. For each active write integration, it creates a `CatalogIntegrationConfig`
3. The adapter's `add_catalog_integration` method is called with this config
4. The adapter creates and registers a `CatalogIntegration` instance

## 3. What Adapter Maintainers Need to Implement

### Step 1: Create Catalog Integration Classes

Create a catalog integration class that inherits from `CatalogIntegration`:

```python
from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig

class MyIcebergCatalogIntegration(CatalogIntegration):
    catalog_type = "iceberg_rest"  # Must match catalog_type in catalogs.yml
    allows_writes = True  # Set to True for write integrations
    table_format = "iceberg"  # Default table format
    file_format = "parquet"  # Default file format

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        # Handle adapter-specific properties
        self.storage_uri = config.adapter_properties.get("storage_uri")
        self.custom_property = config.adapter_properties.get("custom_property")

    def build_relation(self, model: RelationConfig) -> MyCatalogRelation:
        """Build a relation object for this catalog integration"""
        return MyCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=self.external_volume,
            storage_uri=self._calculate_storage_uri(model),
        )

    def _calculate_storage_uri(self, model: RelationConfig) -> str:
        # Custom logic to determine storage URI
        if model.config and model.config.get("storage_uri"):
            return model.config["storage_uri"]
        return f"{self.storage_uri}/{model.schema}/{model.name}"
```

### Step 2: Create Catalog Relation Classes

Create a relation class to represent catalog-specific relation configuration:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class MyCatalogRelation:
    catalog_type: str
    catalog_name: Optional[str]
    table_format: Optional[str]
    file_format: Optional[str]
    external_volume: Optional[str]
    storage_uri: Optional[str]

    @property
    def ddl_properties(self) -> Dict[str, str]:
        """Return properties for DDL generation"""
        properties = {}
        if self.table_format:
            properties["table_format"] = self.table_format
        if self.storage_uri:
            properties["location"] = self.storage_uri
        return properties
```

### Step 3: Register Catalog Integrations in Your Adapter

Update your adapter's `__init__.py` to register supported catalog integrations:

```python
from dbt.adapters.base import BaseAdapter
from .catalogs import MyIcebergCatalogIntegration, MyGlueCatalogIntegration

class MyAdapter(BaseAdapter):
    # ... existing code ...

    CATALOG_INTEGRATIONS = [
        MyIcebergCatalogIntegration,
        MyGlueCatalogIntegration,
    ]

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        # Add any default catalog integrations
        self.add_catalog_integration(DEFAULT_CATALOG_CONFIG)
```

### Step 4: Update Materialization Macros

Update your materialization macros to handle catalog configurations:

```sql
-- macros/materializations/table.sql
{% macro my_adapter__create_table_as(temporary, relation, compiled_sql, language='sql') -%}

  {% if config.get('catalog_name') %}
    {% set catalog_relation = adapter.build_catalog_relation(config.model) %}
    {% set ddl_properties = catalog_relation.ddl_properties %}

    create table {{ relation }}
    {% if ddl_properties %}
      {% for key, value in ddl_properties.items() %}
        {{ key }} = '{{ value }}'
      {% endfor %}
    {% endif %}
    as (
      {{ compiled_sql }}
    )
  {% else %}
    -- Standard table creation
    create table {{ relation }} as (
      {{ compiled_sql }}
    )
  {% endif %}

{%- endmacro %}
```

### Step 5: Add Catalog-Aware Model Parsing

Create parsing utilities to extract catalog information from models:

```python
# parse_model.py
from typing import Optional
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME

def catalog_name(model: RelationConfig) -> Optional[str]:
    """Extract catalog name from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    if catalog := model.config.get(CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return catalog

    # Handle legacy 'catalog' config key
    if catalog := model.config.get("catalog"):
        return catalog

    return None

def storage_uri(model: RelationConfig) -> Optional[str]:
    """Extract storage URI from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    return model.config.get("storage_uri")
```

## 4. How to Test Your Catalog Integration

### Step 1: Create Test Base Class

Create a test class that extends `BaseCatalogIntegrationValidation`:

```python
# tests/functional/adapter/catalog_integrations/test_catalog_integration.py
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation
)

class TestMyAdapterCatalogIntegration(BaseCatalogIntegrationValidation):

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_iceberg_catalog",
                    "active_write_integration": "iceberg_integration",
                    "write_integrations": [
                        {
                            "name": "iceberg_integration",
                            "catalog_type": "iceberg_rest",
                            "table_format": "iceberg",
                            "external_volume": "s3://test-bucket/",
                            "adapter_properties": {
                                "storage_uri": "s3://test-bucket/warehouse/",
                                "custom_property": "test_value"
                            }
                        }
                    ]
                }
            ]
        }
```

### Step 2: Create Test Models

Create test models that use your catalog integration:

```python
# Test model that uses catalog configuration
MODEL_WITH_CATALOG = """
{{ config(
    materialized='table',
    catalog_name='test_iceberg_catalog'
) }}

select 1 as id, 'test' as name
"""

# Test model with additional catalog-specific configs
MODEL_WITH_CATALOG_CONFIGS = """
{{ config(
    materialized='table',
    catalog_name='test_iceberg_catalog',
    storage_uri='s3://test-bucket/custom/path/',
    custom_table_property='custom_value'
) }}

select 1 as id, 'test' as name
"""
```

### Step 3: Integration Tests

Create integration tests that verify catalog functionality:

```python
def test_catalog_integration_creates_table(self, project):
    """Test that catalog integration successfully creates tables"""
    # Create model with catalog configuration
    write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")

    # Run dbt and verify success
    results = run_dbt(["run"])
    assert len(results) == 1
    assert results[0].status == "success"

    # Verify table was created in correct location
    # Add adapter-specific verification logic here

def test_catalog_integration_with_custom_properties(self, project):
    """Test catalog integration with custom properties"""
    write_file(MODEL_WITH_CATALOG_CONFIGS, project.project_root, "models", "test_model.sql")

    results = run_dbt(["run"])
    assert len(results) == 1
    assert results[0].status == "success"

    # Verify custom properties were applied
    # Add verification logic here
```

### Step 4: Unit Tests

Create unit tests for your catalog integration classes:

```python
# tests/unit/test_catalog_integrations.py
import unittest
from unittest.mock import Mock

from dbt.adapters.myplatform.catalogs import MyIcebergCatalogIntegration

class TestMyIcebergCatalogIntegration(unittest.TestCase):

    def setUp(self):
        self.config = Mock()
        self.config.name = "test_catalog"
        self.config.catalog_type = "iceberg_rest"
        self.config.catalog_name = "my_catalog"
        self.config.table_format = "iceberg"
        self.config.external_volume = "s3://bucket/"
        self.config.adapter_properties = {"storage_uri": "s3://bucket/warehouse/"}

    def test_integration_initialization(self):
        """Test catalog integration initializes correctly"""
        integration = MyIcebergCatalogIntegration(self.config)

        assert integration.name == "test_catalog"
        assert integration.catalog_type == "iceberg_rest"
        assert integration.table_format == "iceberg"
        assert integration.allows_writes is True

    def test_build_relation(self):
        """Test build_relation method"""
        integration = MyIcebergCatalogIntegration(self.config)

        model = Mock()
        model.schema = "test_schema"
        model.name = "test_model"
        model.config = {}

        relation = integration.build_relation(model)

        assert relation.catalog_type == "iceberg_rest"
        assert relation.table_format == "iceberg"
        assert "s3://bucket/warehouse/" in relation.storage_uri
```

## Best Practices

### 1. Error Handling

Implement proper error handling for catalog operations:

```python
from dbt_common.exceptions import DbtConfigError

def build_relation(self, model: RelationConfig) -> MyCatalogRelation:
    try:
        # Build relation logic
        return MyCatalogRelation(...)
    except Exception as e:
        raise DbtConfigError(
            f"Failed to build catalog relation for model {model.name}: {str(e)}"
        )
```

### 2. Validation

Add validation for catalog configurations:

```python
def __init__(self, config: CatalogIntegrationConfig) -> None:
    super().__init__(config)

    # Validate required properties
    if not self.external_volume:
        raise DbtConfigError(
            f"external_volume is required for catalog integration '{self.name}'"
        )

    # Validate adapter properties
    if required_prop := config.adapter_properties.get("required_property"):
        if not self._validate_property(required_prop):
            raise DbtConfigError(f"Invalid required_property: {required_prop}")
```

### 3. Documentation

Document your catalog integration configuration options:

```python
class MyIcebergCatalogIntegration(CatalogIntegration):
    """
    Iceberg catalog integration for MyPlatform

    Supported adapter_properties:
    - storage_uri: Base URI for table storage (required)
    - custom_property: Custom configuration option (optional)

    Example configuration:
    ```yaml
    catalogs:
      - name: my_iceberg
        write_integrations:
          - name: iceberg_integration
            catalog_type: iceberg_rest
            adapter_properties:
              storage_uri: s3://my-bucket/warehouse/
              custom_property: value
    ```
    """
```

## Additional Resources

- [dbt Core catalog integration discussion](https://github.com/dbt-labs/dbt-core/discussions/11171)
- [Snowflake Iceberg catalog integration docs](https://docs.getdbt.com/docs/mesh/iceberg/snowflake-iceberg-support#dbt-catalog-integration-configurations-for-snowflake)
- [dbt Core v1.10 upgrade guide](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v1.10)
- [External data source documentation](https://docs.getdbt.com/reference/resource-properties/external)

## Example Implementations

Reference implementations in existing adapters:

- **Snowflake**: [`dbt-snowflake` catalog integrations](https://github.com/dbt-labs/dbt-snowflake/tree/main/dbt/adapters/snowflake/catalogs)
- **BigQuery**: [`dbt-bigquery` catalog integrations](https://github.com/dbt-labs/dbt-bigquery/tree/main/dbt/adapters/bigquery/catalogs)
- **Databricks**: [`dbt-databricks` catalog integrations](https://github.com/databricks/dbt-databricks/tree/main/dbt/adapters/databricks/catalogs)

This feature enables dbt users to work with external catalogs in a standardized way while giving adapter maintainers flexibility to implement platform-specific catalog behaviors.
