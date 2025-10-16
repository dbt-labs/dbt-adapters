{% macro snowflake__get_replace_hybrid_table_sql(existing_relation, target_relation, sql) -%}
{#-
    Replace an existing hybrid table

    This is used for full refresh scenarios
-#}
    {{- log('Replacing hybrid table: ' ~ existing_relation) -}}

    {#- Drop the old relation first -#}
    {{ snowflake__get_drop_hybrid_table_sql(existing_relation) }};

    {#- Create the new hybrid table -#}
    {{ snowflake__get_create_hybrid_table_as_sql(target_relation, sql) }}

{%- endmacro %}
