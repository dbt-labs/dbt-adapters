{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }}{% if not redshift__drop_without_cascade() %} cascade{% endif %}
{%- endmacro %}
