{%- macro redshift__drop_table(relation) -%}
    drop table if exists {{ relation }}{% if not redshift__drop_without_cascade() %} cascade{% endif %}
{%- endmacro -%}
