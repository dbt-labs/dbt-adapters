{#
    Data metric function (DMF) management.

    Mirrors the `apply_grants` "converge after build" pattern: the current DMFs and
    schedule are read from Snowflake, diffed against the model config in Python, and
    the resulting ALTER statements are executed. The diff logic lives in
    `SnowflakeRelation.data_metric_function_changeset` so it can be unit tested; these
    macros stay thin and only render DDL.
#}

{% macro snowflake__get_set_data_metric_schedule_sql(relation, schedule) %}
    alter table {{ relation.render() }} set data_metric_schedule = '{{ schedule }}'
{%- endmacro %}


{% macro snowflake__get_add_data_metric_function_sql(relation, data_metric_function) %}
    alter table {{ relation.render() }} add data metric function {{ data_metric_function.name }} on ({{ data_metric_function.arguments_clause }})
{%- endmacro %}


{% macro snowflake__get_drop_data_metric_function_sql(relation, data_metric_function) %}
    alter table {{ relation.render() }} drop data metric function {{ data_metric_function.name }} on ({{ data_metric_function.arguments_clause }})
{%- endmacro %}


{% macro snowflake__apply_data_metric_functions(relation) %}

    {%- set schedule = config.get('data_metric_schedule') -%}
    {%- set data_metric_functions = config.get('data_metric_functions') -%}

    {#-- No DMF configuration on this model: nothing to converge. --#}
    {%- if not schedule and not data_metric_functions -%}
        {%- do return(none) -%}
    {%- endif -%}

    {%- set existing_data_metric_functions = adapter.describe_data_metric_functions(relation) -%}
    {%- set changeset = relation.data_metric_function_changeset(existing_data_metric_functions, config.model) -%}

    {%- if changeset is none -%}
        {{ log("On " ~ relation.render() ~ ": data metric functions already in sync, no changes required.") }}
        {%- do return(none) -%}
    {%- endif -%}

    {%- set alter_statements = [] -%}

    {#-- Snowflake requires a schedule on the relation before any DMF is added, so the
         schedule change is emitted before the adds. --#}
    {%- if changeset.schedule is not none -%}
        {%- do alter_statements.append(snowflake__get_set_data_metric_schedule_sql(relation, changeset.schedule)) -%}
    {%- endif -%}

    {%- for data_metric_function in changeset.to_drop -%}
        {%- do alter_statements.append(snowflake__get_drop_data_metric_function_sql(relation, data_metric_function)) -%}
    {%- endfor -%}

    {%- for data_metric_function in changeset.to_add -%}
        {%- do alter_statements.append(snowflake__get_add_data_metric_function_sql(relation, data_metric_function)) -%}
    {%- endfor -%}

    {% call statement('data_metric_functions') -%}
        {% for alter_statement in alter_statements %}
            {{ alter_statement }};
        {% endfor %}
    {%- endcall %}

{% endmacro %}
