{% macro redshift__drop_materialized_view(relation) -%}
    {%- set without_cascade = config.get('drop_without_cascade', default=none) -%}
    {%- if without_cascade is none -%}
        {%- set without_cascade = redshift__drop_without_cascade() -%}
    {%- endif -%}
    drop materialized view if exists {{ relation }} {% if not without_cascade %}cascade{% endif %}
{%- endmacro %}
