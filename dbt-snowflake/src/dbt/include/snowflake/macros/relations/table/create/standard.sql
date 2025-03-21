{% macro snowflake__create_table_standard_sql(temporary, relation, compiled_code) -%}
{#-
    Implements CREATE TABLE and CREATE TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-table
    https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas
-#}

{%- set materialization_prefix = relation.get_ddl_prefix_for_create(config.model.config, temporary) -%}

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
    alter table {{relation}} cluster by ({{cluster_by_string}});
  {%- endif -%}
  {% if enable_automatic_clustering and cluster_by_string is not none and not temporary %}
    alter table {{relation}} resume recluster;
  {%- endif -%}

{% endmacro %}
