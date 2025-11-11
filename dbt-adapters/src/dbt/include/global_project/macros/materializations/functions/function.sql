{% materialization function, default, supported_languages=['sql', 'python'] %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.Function) %}

    {{ run_hooks(pre_hooks) }}

    {% set function_config = this.get_function_config(model) %}
    {% set macro_name = this.get_function_macro_name(function_config) %}

    {# Doing this aliasing of adapter.dispatch is a hacky way to disable the static analysis of actually calling adapter.dispatch #}
    {# This is necessary because the static analysis breaks being able to dynamically pass a macro_name #}
    {% set _dispatch = adapter.dispatch %}

    {% set build_sql = _dispatch(macro_name, 'dbt')(target_relation) %}
    {{ function_execute_build_sql(build_sql, existing_relation, target_relation) }}
    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
