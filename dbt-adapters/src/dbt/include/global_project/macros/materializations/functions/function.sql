{% materialization function, default %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.Function) %}

    {{ run_hooks(pre_hooks) }}

    {% set function_type_macro_name = "function_aggregate" ~ language}
    {% set function_type_macro = context[function_type_macro_name] %}
    {% set build_sql = function_type_macro(target_relation) %}

    {{ function_execute_build_sql(build_sql, existing_relation, target_relation) }}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
