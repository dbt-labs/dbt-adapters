{% macro athena__create_view_as(relation, sql) -%}
  {%- set contract_config = config.get('contract') -%}
  {%- set is_data_catalog_view = config.get('is_data_catalog_view', False) -%}

  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif -%}

  {%- if is_data_catalog_view -%}
  create or replace protected multi dialect view
    {{ relation }}
  security definer
  as
    {{ sql }}
  {%- else -%}
  create or replace view
    {{ relation }}
  as
    {{ sql }}
  {%- endif -%}
{% endmacro %}
