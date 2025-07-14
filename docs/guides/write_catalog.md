# Implementing Support for Catalog Integrations in dbt Core Adapters

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
  - **`catalog_type`**: Type of catalog (e.g., "iceberg_rest", "unity", "glue"), this is how you will distinguish between different types of integrations.
  - **`catalog_name`**: Name of the catalog in the data platform
  - **`table_format`**: Table format (e.g., "iceberg", "delta")
  - **`external_volume`**: External storage volume identifier, use this in the context of the integration (i.e. it can be an s3 path or the logical name of a drive or configuration)
  - **`file_format`**: File format (e.g., "parquet", "delta")
  - **`adapter_properties`**: Adapter-specific properties, a dictionary of platform configuration parameters

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

### Step 3: Define Constants for Your Catalog Integrations (Optional)

Following the established pattern in existing adapters, you can optionally create a `constants.py` file to define your catalog types and default configurations. This step is recommended if you have multiple catalog integrations or platform-specific parameters, but you can use your best judgment based on your adapter's complexity:

```python
# constants.py
from types import SimpleNamespace

# Define string constants for catalog types
INFO_SCHEMA_CATALOG_TYPE = "INFO_SCHEMA"
ICEBERG_CATALOG_TYPE = "iceberg_rest"
GLUE_CATALOG_TYPE = "glue"

# Define table format constants
DEFAULT_TABLE_FORMAT = "default"
ICEBERG_TABLE_FORMAT = "iceberg"

# Define file format constants
DEFAULT_FILE_FORMAT = "default"
PARQUET_FILE_FORMAT = "parquet"

# Define default catalog integrations using SimpleNamespace
DEFAULT_INFO_SCHEMA_CATALOG = SimpleNamespace(
    name="info_schema",
    catalog_name="info_schema",
    catalog_type=INFO_SCHEMA_CATALOG_TYPE,
    table_format=DEFAULT_TABLE_FORMAT,
    external_volume=None,
    file_format=DEFAULT_FILE_FORMAT,
    adapter_properties={},
)

DEFAULT_ICEBERG_CATALOG = SimpleNamespace(
    name="managed_iceberg",
    catalog_name="managed_iceberg",
    catalog_type=ICEBERG_CATALOG_TYPE,
    table_format=ICEBERG_TABLE_FORMAT,
    external_volume=None,
    file_format=PARQUET_FILE_FORMAT,
    adapter_properties={},
)

# Platform-specific parameter names (if needed)
MyPlatformIcebergTableParameters = SimpleNamespace(
    storage_uri="storage_uri",
    custom_property="custom_property",
)
```

This pattern provides several benefits:

- **Consistency**: String constants ensure consistent naming across your adapter
- **Default Configurations**: SimpleNamespace objects provide default catalog configurations
- **Easy References**: Constants can be imported and used throughout your adapter code
- **Platform Parameters**: Centralized location for platform-specific parameter names

**When to use this pattern:**
- You have multiple catalog integrations with different types
- Your platform has many configuration parameters that need consistent naming
- You want to provide default catalog configurations for common use cases
- You're building a complex adapter with multiple files referencing the same parameters

**When it might be overkill:**
- You only have one simple catalog integration
- Your configuration parameters are straightforward and unlikely to change
- You prefer to define configurations inline for simplicity

### Step 4: Register Catalog Integrations in Your Adapter

Update your adapter's `__init__.py` to register supported catalog integrations:

**With constants pattern:**
```python
from dbt.adapters.base import BaseAdapter
from .catalogs import MyIcebergCatalogIntegration, MyGlueCatalogIntegration
from . import constants

class MyAdapter(BaseAdapter):
    # ... existing code ...

    CATALOG_INTEGRATIONS = [
        MyIcebergCatalogIntegration,
        MyGlueCatalogIntegration,
    ]

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        # Add default catalog integrations using constants
        self.add_catalog_integration(constants.DEFAULT_INFO_SCHEMA_CATALOG)
        self.add_catalog_integration(constants.DEFAULT_ICEBERG_CATALOG)
```

**Without constants pattern (simpler approach):**
```python
from dbt.adapters.base import BaseAdapter
from .catalogs import MyIcebergCatalogIntegration
from types import SimpleNamespace

class MyAdapter(BaseAdapter):
    # ... existing code ...

    CATALOG_INTEGRATIONS = [
        MyIcebergCatalogIntegration,
    ]

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        # Add a simple default catalog integration inline
        default_catalog = SimpleNamespace(
            name="default_catalog",
            catalog_type="iceberg_rest",
            catalog_name="default_catalog",
            table_format="iceberg",
            external_volume=None,
            file_format="parquet",
            adapter_properties={},
        )
        self.add_catalog_integration(default_catalog)
```

### Constants Pattern Examples

Here are real examples from existing adapters:

**BigQuery** ([`constants.py`](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/adapters/bigquery/constants.py)):
```python
BIGLAKE_CATALOG_TYPE = "biglake_metastore"
ICEBERG_TABLE_FORMAT = "iceberg"
PARQUET_FILE_FORMAT = "parquet"

DEFAULT_ICEBERG_CATALOG = SimpleNamespace(
    name="managed_iceberg",
    catalog_name="managed_iceberg",
    catalog_type=BIGLAKE_CATALOG_TYPE,
    table_format=ICEBERG_TABLE_FORMAT,
    external_volume=None,
    file_format=PARQUET_FILE_FORMAT,
    adapter_properties={},
)
```

**Snowflake** ([`constants.py`](https://github.com/dbt-labs/dbt-snowflake/blob/main/dbt/adapters/snowflake/constants.py)):
```python
ICEBERG_TABLE_FORMAT = "ICEBERG"

DEFAULT_BUILT_IN_CATALOG = SimpleNamespace(
    name="SNOWFLAKE",
    catalog_type="BUILT_IN",
    external_volume=None,
    file_format=None,
    adapter_properties={},
)

SnowflakeIcebergTableRelationParameters = SimpleNamespace(
    storage_serialization_policy="storage_serialization_policy",
    data_retention_time_in_days="data_retention_time_in_days",
    max_data_extension_time_in_days="max_data_extension_time_in_days",
    change_tracking="change_tracking",
)
```

### Step 5: Update Materialization Macros

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

### Step 6: Add Catalog-Aware Model Parsing

Create parsing utilities to extract catalog information from models:

**With constants pattern:**
```python
# parse_model.py
from typing import Optional
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME
from . import constants

def catalog_name(model: RelationConfig) -> Optional[str]:
    """Extract catalog name from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    if catalog := model.config.get(CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return catalog

    # Handle legacy 'catalog' config key
    if catalog := model.config.get("catalog"):
        return catalog

    return constants.DEFAULT_INFO_SCHEMA_CATALOG.name

def storage_uri(model: RelationConfig) -> Optional[str]:
    """Extract storage URI from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    return model.config.get(constants.MyPlatformIcebergTableParameters.storage_uri)
```

**Without constants pattern:**
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

    return "default_catalog"  # Simple default

def storage_uri(model: RelationConfig) -> Optional[str]:
    """Extract storage URI from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    return model.config.get("storage_uri")  # Direct parameter name
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
