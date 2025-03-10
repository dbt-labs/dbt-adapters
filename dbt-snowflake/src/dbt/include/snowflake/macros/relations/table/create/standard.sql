{% macro snowflake__create_table_standard_sql(temporary, relation, compiled_code) -%}
{#-
    Implements CREATE TABLE and CREATE TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-table
-#}

{%- set materialization_prefix = relation.get_ddl_prefix_for_create(config.model.config, temporary) -%}

{%- set sql_header = config.get('sql_header', none) -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
{%- if not temporary and cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_string = cluster_by_keys -%}
{%- elif not temporary and cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ") -%}
{%- else -%}
    {%- set cluster_by_string = none -%}
{%- endif -%}
{%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(compiled_code) }}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{{ sql_header if sql_header is not none }}

create or replace {{ materialization_prefix }} table {{ relation }}
    {% if contract_config.enforced -%}
        {{ get_table_columns_and_constraints() }}
    {%- endif %}

    {{ optional('cluster by', cluster_by_string, "(") }}
    {% if copy_grants and not temporary -%}copy grants{%- endif %}
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

{% if not temporary and enable_automatic_clustering and cluster_by_string is not none %}
alter table {{ relation }} resume recluster;
{%- endif -%}

{% endmacro %}
