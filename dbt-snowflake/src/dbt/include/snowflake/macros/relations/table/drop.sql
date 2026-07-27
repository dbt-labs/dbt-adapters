{% macro snowflake__drop_table(relation) %}
    {#-- CASCADE is not supported in catalog-linked databases --#}

    {% if snowflake__is_catalog_linked_database(relation=relation) %}
        drop table if exists {{ relation }}
    {% else %}
        drop table if exists {{ relation }} cascade
    {% endif %}
{% endmacro %}
