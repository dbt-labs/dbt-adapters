{% macro snowflake__create_table_iceberg_managed_sql(relation, compiled_code) -%}
{#-
    Implements CREATE ICEBERG TABLE and CREATE ICEBERG TABLE ... AS SELECT (Snowflake as the Iceberg catalog):
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake

    Limitations:
    - Iceberg does not support temporary tables (use a standard Snowflake table)
-#}

{%- if not adapter.behavior.enable_iceberg_materializations.no_warn -%}
    {%- do exceptions.raise_compiler_error('Was unable to create model as Iceberg Table Format. Please set the `enable_iceberg_materializations` behavior flag to True in your dbt_project.yml. For more information, go to https://docs.getdbt.com/reference/resource-configs/snowflake-configs#iceberg-table-format') -%}
{%- endif -%}

{%- set _catalog = adapter.build_catalog_relation(model) -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace iceberg table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {{ optional('cluster by', _catalog.cluster_by, '(', '') }}
    {{ optional('external_volume', _catalog.external_volume, "'") }}
    catalog = 'snowflake'
    base_location = '{{ _catalog.base_location }}'
    {% if copy_grants %}copy grants{% endif %}
as (
    {%- if _catalog.cluster_by is not none -%}
    select * from (
        {{ compiled_code }}
    )
    order by (
        {{ _catalog.cluster_by }}
    )
    {%- else -%}
    {{ compiled_code }}
    {%- endif %}
);

{% if _catalog.cluster_by is not none -%}
alter iceberg table {{ relation }} cluster by ({{ _catalog.cluster_by }});
{%- endif -%}

{% if _catalog.automatic_clustering and _catalog.cluster_by is not none %}
alter iceberg table {{ relation }} resume recluster;
{%- endif -%}

{%- endmacro %}
