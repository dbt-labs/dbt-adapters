{% macro snowflake__create_table_standard_sql(relation, compiled_code) -%}
{#-
    Implements CREATE TABLE and CREATE TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-table
    https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas
-#}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

{%- if catalog_relation.is_transient -%}
    {%- set transient='transient ' -%}
{%- else -%}
    {%- set transient='' -%}
{%- endif -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace {{ transient }}table {{ relation }}
    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {{ optional('cluster by', catalog_relation.cluster_by, '(', '') }}
    {% if copy_grants -%} copy grants {%- endif %}
as (
    {%- if catalog_relation.cluster_by is not none -%}
    select * from (
        {{ compiled_code }}
    )
    order by (
        {{ catalog_relation.cluster_by }}
    )
    {%- else -%}
    {{ compiled_code }}
    {%- endif %}
);

{% if catalog_relation.cluster_by is not none -%}
alter table {{ relation }} cluster by ({{ catalog_relation.cluster_by }});
{%- endif -%}

{% if catalog_relation.automatic_clustering and catalog_relation.cluster_by is not none %}
alter table {{ relation }} resume recluster;
{%- endif -%}

{%- endmacro %}
