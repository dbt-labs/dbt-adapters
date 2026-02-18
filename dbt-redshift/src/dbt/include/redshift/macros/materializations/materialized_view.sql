{% materialization materialized_view, adapter='redshift' %}
    {% set original_query_group = set_query_group() %}

    {% set relations = materialization_materialized_view_default() %}

    {% do unset_query_group(original_query_group) %}
    {{ return(relations) }}
{% endmaterialization %}


{% macro redshift__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = redshift__describe_materialized_view(existing_relation) %}
    {% set _configuration_changes = existing_relation.materialized_view_config_changeset(_existing_materialized_view, new_config.model) %}
    {% do return(_configuration_changes) %}
{% endmacro %}
