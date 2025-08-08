{% macro snowflake__generate_database_name(custom_database_name=none, node=none) -%}
    {%- if custom_database_name is none -%}
         {%- if node is not none -%}
            {%- set catalog_relation = adapter.build_catalog_relation(node) -%}
        {%- elif 'config' in target -%}
            {%- set catalog_relation = adapter.build_catalog_relation(target) -%}

        {%- endif -%}
        {%- if catalog_relation is not none
            and catalog_relation|attr('catalog_linked_database')-%}
            {{ return(catalog_relation.catalog_linked_database) }}
        {%- else -%}
            {{ return(target.database) }}
        {%- endif -%}
    {%- else -%}
       {{ return(custom_database_name) }}
    {%- endif -%}
{%- endmacro %}
