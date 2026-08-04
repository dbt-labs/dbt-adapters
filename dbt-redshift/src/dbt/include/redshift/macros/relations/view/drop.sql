{%- macro redshift__drop_view(relation) -%}
    drop view if exists {{ relation }}{% if not redshift__drop_without_cascade() %} cascade{% endif %}
{%- endmacro -%}
