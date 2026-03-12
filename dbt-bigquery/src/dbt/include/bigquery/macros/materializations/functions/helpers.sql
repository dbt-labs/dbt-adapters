{% macro bigquery__function_execute_build_sql(build_sql, existing_relation, target_relation) %}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {% if config.get('grant_access_to') %}
      {% for grant_target_dict in config.get('grant_access_to') %}
        {% do adapter.grant_access_to(this, 'routine', None, grant_target_dict) %}
      {% endfor %}
    {% endif %}

    {{ adapter.commit() }}

{% endmacro %}
