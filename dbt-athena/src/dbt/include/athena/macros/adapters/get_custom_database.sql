{#
  Priority: catalog_database (from catalogs.yml) > custom_database_name (model `database` config) > target.database
#}
{% macro athena__generate_database_name(custom_database_name=none, node=none) -%}
    {%- if node is not none -%}
        {%- set catalog_relation = adapter.build_catalog_relation(node) -%}
    {%- else -%}
        {%- set catalog_relation = none -%}
    {%- endif -%}
    {%- if catalog_relation is not none
        and catalog_relation|attr('catalog_database') -%}
        {{ return(catalog_relation.catalog_database) }}
    {%- elif custom_database_name is not none -%}
        {{ return(custom_database_name) }}
    {%- else -%}
        {{ return(target.database) }}
    {%- endif -%}
{%- endmacro %}
