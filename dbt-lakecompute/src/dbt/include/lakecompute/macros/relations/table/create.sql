{#- no trailing `;`: it would make the request a batch with an empty second statement -#}
{#- SQL only: no materialization here declares `python`, so dbt rejects Python models first -#}
{% macro lakecompute__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if temporary -%}
    {{ exceptions.raise_compiler_error(
      "lakecompute has no temporary tables (tried to create " ~ relation.render()
      ~ " as temporary)."
    ) }}
  {%- endif -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(compiled_code) }}
    {#- renders nothing here; called for dbt's per-constraint "not supported" warning -#}
    {%- do adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {%- do adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
  {%- endif %}

create or replace table {{ relation.render() }} as (
{{ compiled_code }}
)
{%- endmacro %}
