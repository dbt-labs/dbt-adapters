{% macro snowflake__attach_interactive_warehouses(relation) %}
    {%- set warehouses = config.get('snowflake_interactive_warehouses') -%}
    {%- if warehouses -%}
        {%- set warehouses = [warehouses] if warehouses is string else warehouses -%}
        {%- for warehouse in warehouses -%}
            {%- call statement('attach_interactive_warehouse_' ~ loop.index) -%}
                alter warehouse {{ warehouse }} add tables ({{ relation }})
            {%- endcall -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
