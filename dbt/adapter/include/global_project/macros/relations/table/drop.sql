{# /*
This was already implemented. Instead of creating a new macro that aligns with the standard,
this was reused and the default was maintained. This gets called by `drop_relation`, which
actually executes the drop, and `get_drop_sql`, which returns the template.
*/ #}

{% macro drop_table(relation) -%}
    {{ return(adapter.dispatch('drop_table', 'dbt')(relation)) }}
{%- endmacro %}


{% macro default__drop_table(relation) -%}
    drop table if exists {{ relation }} cascade
{%- endmacro %}
