{% materialization hybrid_table, adapter='snowflake' %}

    {% set query_tag = set_query_tag() %}

    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.HybridTable) %}

    {{ run_hooks(pre_hooks) }}

    {% set build_sql = hybrid_table_get_build_sql(existing_relation, target_relation) %}

    {% if build_sql == '' %}
        {{ hybrid_table_execute_no_op(target_relation) }}
    {% else %}
        {{ hybrid_table_execute_build_sql(build_sql, existing_relation, target_relation) }}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {% do unset_query_tag(query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro hybrid_table_get_build_sql(existing_relation, target_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, merge, or handle schema changes
    {% if existing_relation is none %}
        {% set build_sql = snowflake__get_create_hybrid_table_as_sql(target_relation, sql) %}

    {% elif full_refresh_mode or not existing_relation.is_hybrid_table %}
        {% set build_sql = snowflake__get_replace_hybrid_table_sql(existing_relation, target_relation, sql) %}

    {% else %}
        -- Table exists and is a hybrid table, check for configuration changes
        {% set on_schema_change = config.get('on_schema_change', 'fail') %}
        {% set configuration_changes = snowflake__get_hybrid_table_configuration_changes(existing_relation, config) %}

        {% if configuration_changes is none %}
            -- No schema changes detected, perform incremental merge
            {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
            {% set dest_column_names = dest_columns | map(attribute='name') | list %}
            {% set build_sql = snowflake__get_merge_sql_hybrid_table(target_relation, sql, none, dest_column_names) %}

        {% elif on_schema_change == 'fail' %}
            {{ exceptions.raise_compiler_error(
                "Schema or configuration changes detected on hybrid table `" ~ target_relation ~
                "`. Hybrid tables have limited ALTER support. Set `on_schema_change` to 'continue' to proceed with warnings, or 'apply' to force a full refresh."
            ) }}

        {% elif on_schema_change == 'apply' %}
            {{- log('Configuration changes detected on: ' ~ target_relation ~ '. Performing full refresh.') -}}
            {% set build_sql = snowflake__get_replace_hybrid_table_sql(existing_relation, target_relation, sql) %}

        {% elif on_schema_change == 'continue' %}
            {{ exceptions.warn(
                "Configuration changes were identified on `" ~ target_relation ~
                "` but `on_schema_change` is set to `continue`. Proceeding with incremental merge without applying changes."
            ) }}
            {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
            {% set dest_column_names = dest_columns | map(attribute='name') | list %}
            {% set build_sql = snowflake__get_merge_sql_hybrid_table(target_relation, sql, none, dest_column_names) %}

        {% else %}
            {{ exceptions.raise_compiler_error("Unexpected on_schema_change value: `" ~ on_schema_change ~ "`") }}
        {% endif %}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro hybrid_table_execute_no_op(relation) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ relation,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro hybrid_table_execute_build_sql(build_sql, existing_relation, target_relation) %}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

{% endmacro %}


{% macro snowflake__get_hybrid_table_configuration_changes(existing_relation, new_config) -%}
    {#-
        Check if there are any configuration changes that would require a rebuild

        For hybrid tables, most schema changes require a full refresh:
        - Primary key changes
        - Index changes
        - Column type changes
        - Adding/removing columns (if included in key or index)
    -#}

    {% set _existing_hybrid_table = snowflake__describe_hybrid_table(existing_relation) %}
    {% set _configuration_changes = existing_relation.hybrid_table_config_changeset(_existing_hybrid_table, new_config.model) %}
    {% do return(_configuration_changes) %}

{%- endmacro %}
