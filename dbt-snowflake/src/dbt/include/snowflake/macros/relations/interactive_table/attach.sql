{% macro snowflake__attach_interactive_warehouses(relation) %}
    {#
        Each entry is rendered as a verbatim SQL identifier, matching `snowflake_warehouse`:
        write it exactly as Snowflake expects. Bare names fold to uppercase; wrap
        case-sensitive or special-character names in double quotes in the config
        (e.g. '"my-wh"').
    #}
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
