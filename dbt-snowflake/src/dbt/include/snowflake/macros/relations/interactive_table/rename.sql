{%- macro snowflake__get_rename_interactive_table_sql(relation, new_name) -%}
    alter table {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
