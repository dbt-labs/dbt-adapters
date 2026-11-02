{% materialization cortex_search_service, adapter='snowflake' %}

    {% set full_refresh_mode = should_full_refresh() %}
    {% set target_relation = this.incorporate(type=this.CortexSearchService) %}
    {% set search = target_relation.from_config(config.model) %}
    {% do search.warehouse_parameter(target.warehouse) %}
    {% set on_configuration_change = config.get('on_configuration_change', 'apply') %}
    {% if on_configuration_change not in ['apply', 'continue', 'fail'] %}
        {% do exceptions.raise_compiler_error('Unexpected on_configuration_change: ' ~ on_configuration_change) %}
    {% endif %}

    {% set query_tag = set_query_tag() %}
    {{ run_hooks(pre_hooks) }}
    {% set existing_relation = snowflake__get_cortex_search_service(target_relation) %}
    {% set build = cortex_search_service_get_build(existing_relation, target_relation, full_refresh_mode, on_configuration_change) %}

    {% if build['sql'] == '' %}
        {{ cortex_search_service_execute_no_op(target_relation) }}
    {% else %}
        {{ cortex_search_service_execute_build_sql(build['sql']) }}
    {% endif %}

    {{ run_hooks(post_hooks) }}
    {% do apply_grants(target_relation, config.get('grants'), should_revoke=should_revoke(existing_relation, full_refresh_mode=build['full_refresh_mode'])) %}
    {% do unset_query_tag(query_tag) %}
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro cortex_search_service_get_build(existing_relation, target_relation, full_refresh_mode, on_configuration_change) %}
    {% set build_sql = '' %}

    {% if existing_relation is none or full_refresh_mode %}
        {% set build_sql = snowflake__get_create_cortex_search_service_sql(target_relation, sql, full_refresh_mode) %}
    {% else %}
        {% set metadata = snowflake__describe_cortex_search_service(target_relation) %}
        {% set search = target_relation.from_config(config.model) %}
        {% set changes = search.configuration_changes(metadata, target.warehouse) %}
        {% set changed_names = changes['replace'] + (changes['alter'].keys() | list) %}
        {% if changed_names %}
            {% if on_configuration_change == 'fail' %}
                {% do exceptions.raise_compiler_error('Cortex Search configuration changed for ' ~ target_relation.render() ~ ': ' ~ changed_names | join(', ') ~ '. on_configuration_change is set to fail.') %}
            {% elif on_configuration_change == 'continue' %}
                {% do exceptions.warn('Cortex Search configuration changed for ' ~ target_relation.render() ~ ': ' ~ changed_names | join(', ') ~ '. Keeping the existing definition because on_configuration_change is set to continue.') %}
            {% elif changes['replace'] %}
                {% set full_refresh_mode = true %}
                {% set build_sql = snowflake__get_create_cortex_search_service_sql(target_relation, sql, full_refresh_mode) %}
            {% else %}
                {% set build_sql = snowflake__get_alter_cortex_search_service_sql(target_relation, changes['alter']) %}
            {% endif %}
        {% endif %}
    {% endif %}

    {{ return({'sql': build_sql, 'full_refresh_mode': full_refresh_mode}) }}
{% endmacro %}


{% macro cortex_search_service_execute_no_op(relation) %}
    {% do store_raw_result(name='main', message='skip ' ~ relation.render(), code='skip', rows_affected=-1) %}
{% endmacro %}


{% macro cortex_search_service_execute_build_sql(build_sql) %}
    {% call statement('main') -%}
        {{ build_sql }}
    {%- endcall %}
{% endmacro %}
