# A normal Redshift table that the Iceberg models read from.
BASE_TABLE = """
{{ config(materialized="table") }}
select 1 as id, 'a' as name
union all
select 2 as id, 'b' as name
"""

BASE_TABLE_WITH_NEW_ROW = """
{{ config(materialized="table") }}
select 1 as id, 'a' as name
union all
select 2 as id, 'b' as name
union all
select 3 as id, 'c' as name
"""

# Iceberg table via the default-registered `glue` catalog. `external_volume` is the
# S3 base prefix (LOCATION); the schema must be an existing Glue-backed external schema.
ICEBERG_TABLE = """
{{
  config(
    materialized="table",
    schema=var("iceberg_external_schema"),
    catalog_name="glue",
    external_volume=var("iceberg_location"),
  )
}}
select * from {{ ref('base_table') }}
"""

ICEBERG_TABLE_PARTITIONED = """
{{
  config(
    materialized="table",
    schema=var("iceberg_external_schema"),
    catalog_name="glue",
    external_volume=var("iceberg_location"),
    partition_by=["bucket(16, id)"],
    table_properties={"compression_type": "zstd"},
    base_location_subpath="v1",
  )
}}
select * from {{ ref('base_table') }}
"""

ICEBERG_INCREMENTAL = """
{{
  config(
    materialized="incremental",
    schema=var("iceberg_external_schema"),
    catalog_name="glue",
    external_volume=var("iceberg_location"),
    incremental_strategy="append",
    unique_key="id",
  )
}}
select * from {{ ref('base_table') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
"""

# A view over an Iceberg table must be late-binding (bind=false) on Redshift.
VIEW_ON_ICEBERG = """
{{ config(materialized="view", bind=false) }}
select * from {{ ref('iceberg_table') }}
"""

# Iceberg models land in the configured external schema; everything else uses the
# default test schema.
GENERATE_SCHEMA_NAME = """
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
"""
