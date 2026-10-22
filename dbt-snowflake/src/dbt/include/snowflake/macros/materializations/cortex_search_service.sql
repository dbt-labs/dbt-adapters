{% materialization cortex_search_service, adapter='snowflake' %}

    {% set full_refresh_mode = should_full_refresh() %}
    {% set target_relation = this.incorporate(type=this.CortexSearchService) %}

    {% set query_tag = set_query_tag() %}
    {% set existing_relation = adapter.get_cortex_search_service(target_relation) %}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') -%}
        {{ snowflake__get_create_cortex_search_service_sql(target_relation, sql, full_refresh_mode) }}
    {%- endcall %}

    {% if not full_refresh_mode %}
        {{ log("Cortex Search service " ~ target_relation ~ " is created only if missing; an existing service is kept unchanged. Run with --full-refresh to apply a changed definition.", info=True) }}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {% do apply_grants(target_relation, config.get('grants'), should_revoke=should_revoke(existing_relation, full_refresh_mode=full_refresh_mode)) %}

    {% do unset_query_tag(query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
