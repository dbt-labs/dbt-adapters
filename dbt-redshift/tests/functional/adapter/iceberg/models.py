SEED_BASE = """
{{
  config(materialized="table")
}}
select 1 as id, 'a' as name
union all
select 2 as id, 'b' as name
"""

# Basic Iceberg table via the default-registered `glue` catalog integration.
# `external_volume` maps to the Iceberg LOCATION clause.
ICEBERG_TABLE = """
{{
  config(
    materialized="table",
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
    catalog_name="glue",
    external_volume=var("iceberg_location"),
    partition_by=["bucket(16, id)"],
    table_properties={"compression_type": "zstd"},
  )
}}
select * from {{ ref('base_table') }}
"""

ICEBERG_INCREMENTAL = """
{{
  config(
    materialized="incremental",
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

# A normal Redshift view that reads from an Iceberg table.
VIEW_ON_ICEBERG = """
{{
  config(materialized="view")
}}
select * from {{ ref('iceberg_table') }}
"""
