# Snowflake Catalog Integrations

This guide explains how to configure catalog integrations for Snowflake, including support for Iceberg REST catalogs like Polaris and AWS Glue Data Catalog.

## Overview

Snowflake supports multiple catalog types for managing Iceberg tables:

- `BUILT_IN`: Snowflake's native managed Iceberg catalog
- `INFO_SCHEMA`: Standard Snowflake information schema (default for non-Iceberg tables)
- `ICEBERG_REST`: REST-compatible Iceberg catalogs (Polaris, AWS Glue Data Catalog, etc.)

## Configuring Iceberg REST Catalogs

### Basic Configuration

To use an Iceberg REST catalog, create a `catalogs.yml` file in your dbt project:

```yaml
catalogs:
  - name: polaris_catalog
    active_write_integration: polaris_write
    write_integrations:
      - name: polaris_write
        catalog_type: iceberg_rest
        catalog_name: POLARIS
        external_volume: my_polaris_volume
        table_format: iceberg
        adapter_properties:
          rest_endpoint: "https://polaris.example.com/api/catalog/v1"
          # Additional REST-specific properties can be added here
```

### Required Configuration

| Property | Description | Required |
|----------|-------------|----------|
| `catalog_type` | Must be set to `iceberg_rest` | Yes |
| `catalog_name` | Name of the catalog in Snowflake | Yes |
| `external_volume` | Snowflake external volume for data storage | Yes |
| `adapter_properties.rest_endpoint` | REST API endpoint for the catalog | Yes |

### Optional Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `table_format` | Table format (should be `iceberg`) | `iceberg` |
| `file_format` | File format for data files | `null` (Snowflake chooses) |

### Adapter Properties

The `adapter_properties` section supports catalog-specific configuration:

```yaml
adapter_properties:
  rest_endpoint: "https://polaris.example.com/api/catalog/v1"
  # Add additional properties as needed for your catalog
  # For example, authentication settings, timeouts, etc.
```

## Using Iceberg REST Catalogs in Models

Once configured, reference the catalog in your model configuration:

```sql
{{ config(
    materialized='table',
    catalog_name='polaris_catalog'
) }}
select * from my_source
```

### Model Configuration Options

You can also specify additional Iceberg-specific options:

```sql
{{ config(
    materialized='table',
    catalog_name='polaris_catalog',
    external_volume='my_custom_volume',
    base_location_root='custom_prefix',
    cluster_by=['column1', 'column2']
) }}
select * from my_source
```

## Examples

### AWS Glue Data Catalog

```yaml
catalogs:
  - name: glue_catalog
    active_write_integration: glue_write
    write_integrations:
      - name: glue_write
        catalog_type: iceberg_rest
        catalog_name: GLUE
        external_volume: s3_glue_volume
        adapter_properties:
          rest_endpoint: "https://glue.us-east-1.amazonaws.com/v1/catalogs/my-catalog"
```

### Polaris Catalog

```yaml
catalogs:
  - name: polaris_catalog
    active_write_integration: polaris_write
    write_integrations:
      - name: polaris_write
        catalog_type: iceberg_rest
        catalog_name: POLARIS
        external_volume: s3_polaris_volume
        adapter_properties:
          rest_endpoint: "https://polaris.example.com/api/catalog/v1"
```

## Requirements

- Snowflake version 7.0 or later
- Properly configured external volume for data storage
- Network connectivity to the REST catalog endpoint
- Appropriate permissions for Snowflake to access the external volume and catalog

## Limitations

- Iceberg REST catalogs must use Parquet file format
- The catalog must be accessible from your Snowflake account
- Some catalog-specific features may not be available through the REST interface

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure the `rest_endpoint` is accessible from Snowflake
2. **Permission Errors**: Verify Snowflake has proper access to the external volume
3. **Catalog Not Found**: Check that the `catalog_name` matches the catalog configuration in your REST service

### Testing Your Configuration

You can test your catalog configuration by running a simple model:

```sql
{{ config(
    materialized='table',
    catalog_name='your_catalog_name'
) }}
select 1 as test_column
```

If the model runs successfully, your catalog integration is working correctly.

### Iceberg REST Catalog Specifics

When using `iceberg_rest` catalog type, dbt will generate SQL using the [`CREATE ICEBERG TABLE ... CATALOG('catalog_name')`](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest) syntax. This enables dbt to write directly to external REST-compatible catalogs like:

- **Polaris/Snowflake Open Catalog**: Use the Polaris REST API endpoint
- **AWS Glue Data Catalog**: When configured with REST API support
- **Unity Catalog**: Using Databricks Unity Catalog REST API
- **Tabular**: Commercial Iceberg catalog service
- **Any custom REST catalog**: That implements the Apache Iceberg REST specification

The generated DDL will look like:
```sql
CREATE OR REPLACE ICEBERG TABLE my_schema.my_table
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'POLARIS'
    BASE_LOCATION = '_dbt/my_schema/my_table'
AS (
    SELECT 1 as test_column
);
```
