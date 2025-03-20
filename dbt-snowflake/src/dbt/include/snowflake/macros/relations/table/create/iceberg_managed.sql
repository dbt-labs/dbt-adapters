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

{%- set sql_header = config.get('sql_header', none) -%}

{%- set catalog_name = config.get('catalog_name') -%}
{%- if catalog_name is none %}
    {%- set catalog_name = adapter.add_managed_catalog_integration(config.model.config) -%}
{%- endif -%}
{%- set catalog_integration = adapter.get_catalog_integration(catalog_name) -%}

{%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
{%- if cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_string = cluster_by_keys -%}
{%- elif cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ") -%}
{% else %}
    {%- set cluster_by_string = none -%}
{%- endif -%}
{%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{{ sql_header if sql_header is not none }}

create iceberg table {{ relation }}
    {% if contract_config.enforced -%}
        {{ get_table_columns_and_constraints() }}
    {%- endif %}

    {{ optional('cluster by', cluster_by_string, "(") }}
    {{ optional('external_volume', catalog_integration.external_volume, "'") }}
    -- catalog = 'snowflake'
    base_location = '{{ catalog_integration.base_location(relation, config.model.config) }}'
as (
    {%- if cluster_by_string is not none -%}
    select * from (
        {{ compiled_code }}
    )
    order by (
        {{ cluster_by_string }}
    )
    {%- else -%}
    {{ compiled_code }}
    {%- endif %}
);

{% if enable_automatic_clustering and cluster_by_string is not none %}
alter iceberg table {{relation}} resume recluster;
{%- endif -%}

{% endmacro %}
