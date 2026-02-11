{% macro snowflake__get_hybrid_table_schema_changes(existing_relation, new_config) %}
    {% if existing_relation is none %}
        {{ return(none) }}
    {% endif %}
    {% set relation_results = snowflake__describe_hybrid_table(existing_relation) %}
    {% set changes = existing_relation.hybrid_table_config_changeset(relation_results, new_config.model) %}
    {{ return(changes) }}
{% endmacro %}


{% macro snowflake__alter_hybrid_table_add_columns(relation, columns) %}
    {% if not columns %}
        {{ return(none) }}
    {% endif %}
    {% for column in columns %}
        {% call statement('hybrid_table_add_column_' ~ loop.index0) %}
            ALTER TABLE {{ relation }} ADD COLUMN {{ column.name }} {{ column.definition }}
        {% endcall %}
    {% endfor %}
{% endmacro %}


{% macro snowflake__alter_hybrid_table_drop_columns(relation, columns) %}
    {% if not columns %}
        {{ return(none) }}
    {% endif %}
    {% for column in columns %}
        {% call statement('hybrid_table_drop_column_' ~ loop.index0) %}
            ALTER TABLE {{ relation }} DROP COLUMN {{ column.name }}
        {% endcall %}
    {% endfor %}
{% endmacro %}


{% macro snowflake__apply_hybrid_table_schema_changes(on_schema_change, relation, schema_changes) %}
    {% if schema_changes is none or not schema_changes.has_changes %}
        {{ return(none) }}
    {% endif %}

    {% if schema_changes.requires_full_refresh %}
        {{ exceptions.raise_compiler_error(
            "Hybrid table column type changes require a full refresh. Run with --full-refresh to apply: "
            ~ relation
        ) }}
    {% endif %}

    {% if on_schema_change == 'fail' %}
        {{ exceptions.raise_compiler_error(
            "Schema changes detected for " ~ relation ~ " and on_schema_change=fail."
        ) }}
    {% elif on_schema_change == 'append_new_columns' %}
        {% if schema_changes.drop_columns %}
            {{ exceptions.raise_compiler_error(
                "on_schema_change='append_new_columns' does not allow dropping columns on " ~ relation
            ) }}
        {% endif %}
        {{ snowflake__alter_hybrid_table_add_columns(relation, schema_changes.add_columns) }}
    {% elif on_schema_change == 'sync_all_columns' %}
        {{ snowflake__alter_hybrid_table_add_columns(relation, schema_changes.add_columns) }}
        {{ snowflake__alter_hybrid_table_drop_columns(relation, schema_changes.drop_columns) }}
    {% endif %}
{% endmacro %}
