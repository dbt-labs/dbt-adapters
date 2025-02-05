{% macro snowflake__create_table_iceberg_rest_sql(relation) -%}
{#-
    Implements CREATE ICEBERG TABLE (Iceberg REST catalog):
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest#syntax

    Implements CREATE ICEBERG TABLE (AWS Glue as the Iceberg catalog)
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-aws-glue#syntax

    Limitations:
    https://docs.snowflake.com/en/user-guide/tables-iceberg#considerations-and-limitations
    - This is a read-only table
    - Clustering is not supported for external catalogs
-#}

{%- set sql_header = config.get('sql_header', none) -%}

{%- set catalog_name = config.get('catalog_name') -%}
{%- set catalog_integration = adapter.get_catalog_integration(catalog_name) -%}
{%- set catalog_table = catalog_integration.table(relation) -%}

{{ sql_header if sql_header is not none }}

create iceberg table {{ relation }}
    {{ optional('external_volume', catalog_integration.external_volume, "'") }}
    {{ optional('catalog', catalog_integration.name, "'") }}
    catalog_table_name = '{{ catalog_table.catalog_table_name }}'
    {{ optional('catalog_namespace', catalog_integration.namespace, "'") }}
    {{ optional('replace_invalid_characters', catalog_table.replace_invalid_characters) }}
    {{ optional('auto_refresh', catalog_table.auto_refresh) }}
);

{% endmacro %}
