{% macro snowflake__create_table_standard_sql(temporary, relation, compiled_code) -%}

{%- if relation.is_iceberg_format and not adapter.behavior.enable_iceberg_materializations.no_warn %}
{% do exceptions.raise_compiler_error('Was unable to create model as Iceberg Table Format. Please set the `enable_iceberg_materializations` behavior flag to True in your dbt_project.yml. For more information, go to https://docs.getdbt.com/reference/resource-configs/snowflake-configs#iceberg-table-format') %}
{%- endif %}

{%- set materialization_prefix = relation.get_ddl_prefix_for_create(config.model.config, temporary) -%}
{%- set alter_prefix = relation.get_ddl_prefix_for_alter() -%}

{%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
{%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- if cluster_by_keys is not none and cluster_by_keys is string -%}
{%- set cluster_by_keys = [cluster_by_keys] -%}
{%- endif -%}
{%- if cluster_by_keys is not none -%}
{%- set cluster_by_string = cluster_by_keys|join(", ")-%}
{% else %}
{%- set cluster_by_string = none -%}
{%- endif -%}
{%- set sql_header = config.get('sql_header', none) -%}

{{ sql_header if sql_header is not none }}

create or replace {{ materialization_prefix }} table {{ relation }}
    {%- if relation.is_iceberg_format %}
      {#
        Valid DDL in CTAS statements. Plain create statements have a different order.
        https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table
      #}
      {{ relation.get_iceberg_ddl_options(config.model.config) }}
    {%- endif -%}

    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced -%}
      {{ get_assert_columns_equivalent(sql) }}
      {{ get_table_columns_and_constraints() }}
      {% set compiled_code = get_select_subquery(compiled_code) %}
    {% endif %}
    {% if copy_grants and not temporary -%} copy grants {%- endif %} as
    (
      {%- if cluster_by_string is not none -%}
        select * from (
          {{ compiled_code }}
          ) order by ({{ cluster_by_string }})
      {%- else -%}
        {{ compiled_code }}
      {%- endif %}
    );
  {% if cluster_by_string is not none and not temporary -%}
    alter {{ alter_prefix }} table {{relation}} cluster by ({{cluster_by_string}});
  {%- endif -%}
  {% if enable_automatic_clustering and cluster_by_string is not none and not temporary %}
    alter {{ alter_prefix }} table {{relation}} resume recluster;
  {%- endif -%}

{% endmacro %}
