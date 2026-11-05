{% macro apply_data_metrics(relation, data_metrics) %}

    {{ return(adapter.dispatch('apply_data_metrics', 'dbt')(relation, data_metrics)) }}

{% endmacro %}


{% macro get_data_metrics_config() %}

    {{ return(adapter.dispatch('get_data_metrics_config', 'dbt')()) }}

{% endmacro %}


{% macro snowflake__get_data_metrics_config() %}

    {{ return(adapter.parse_data_metrics_config(config.model)) }}

{% endmacro %}


{% macro snowflake__get_alter_data_metrics_prefix(relation) %}

    {%- if relation.is_dynamic_table -%}
        {%- set object_type = 'dynamic table' -%}
    {%- elif relation.is_iceberg_format -%}
        {%- set object_type = 'iceberg table' -%}
    {%- elif relation.type == 'view' -%}
        {%- set object_type = 'view' -%}
    {%- else -%}
        {%- set object_type = 'table' -%}
    {%- endif %}
    alter {{ object_type }} {{ relation.render() }}

{% endmacro %}


{% macro snowflake__get_set_data_metric_schedule_sql(relation, schedule) %}

    {% set escaped_schedule = schedule | replace('\\', '\\\\') | replace("'", "''") %}
    {{ snowflake__get_alter_data_metrics_prefix(relation) }} set data_metric_schedule = '{{ escaped_schedule }}'

{% endmacro %}


{% macro snowflake__get_add_data_metric_function_sql(relation, association) %}

    {{ snowflake__get_alter_data_metrics_prefix(relation) }} add data metric function {{ association.function_sql }} on ({{ association.columns_sql }})

{% endmacro %}


{% macro snowflake__get_drop_data_metric_function_sql(relation, association) %}

    {{ snowflake__get_alter_data_metrics_prefix(relation) }} drop data metric function {{ association.function_sql }} on ({{ association.columns_sql }})

{% endmacro %}


{% macro snowflake__apply_data_metrics(relation, data_metrics) %}

    {% if not data_metrics.is_configured %}
        {{ return(none) }}
    {% endif %}

    {% set current_state = adapter.describe_data_metrics(relation, data_metrics) %}
    {% set changeset = relation.data_metrics_config_changeset(current_state, data_metrics) %}

    {% if changeset is none %}
        {{ log("On " ~ relation.render() ~ ": data metric functions already in sync, no changes required.") }}
        {{ return(none) }}
    {% endif %}

    {% set statements = [] %}
    {% if changeset.schedule is not none %}
        {% do statements.append(snowflake__get_set_data_metric_schedule_sql(relation, changeset.schedule)) %}
    {% endif %}
    {% for association in changeset.adds %}
        {% do statements.append(snowflake__get_add_data_metric_function_sql(relation, association)) %}
    {% endfor %}
    {% for association in changeset.drops %}
        {% do statements.append(snowflake__get_drop_data_metric_function_sql(relation, association)) %}
    {% endfor %}

    {% for statement_sql in statements %}
        {% call statement('apply_data_metrics') %}
            {{ statement_sql }}
        {% endcall %}
    {% endfor %}

{% endmacro %}
