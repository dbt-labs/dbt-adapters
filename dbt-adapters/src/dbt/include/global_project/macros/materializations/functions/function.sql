{% materialization function, default %}
    {% if not adapter.supports(adapter.capability_for_string('UDFs')) %}
        {% do exceptions.raise_not_implemented('UDFs are not supported for this adapter') %}
    {% endif %}

    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.Function) %}

    {{ run_hooks(pre_hooks) }}

    {% set build_sql = get_udf_build_sql(target_relation) %}

    {{ function_execute_build_sql(build_sql, existing_relation, target_relation) }}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
