{%- macro redshift__drop_table(relation) -%}
    {%- set without_cascade = config.get('drop_without_cascade', default=none) -%}
    {%- if without_cascade is none -%}
        {%- set without_cascade = redshift__drop_without_cascade() -%}
    {%- endif -%}
    drop table if exists {{ relation }} {% if not without_cascade %}cascade{% endif %}
{%- endmacro -%}
