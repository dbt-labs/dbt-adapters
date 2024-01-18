missing_column = """
{% macro export_table_check() %}

    {% set table = 'test_column' %}

    {% set query %}
        SELECT column_name
        FROM {{ref(table)}}
        LIMIT 1
    {% endset %}

    {%- if flags.WHICH in ('run', 'build') -%}
        {% set results = run_query(query) %}
        {% if execute %}
            {%- if results.rows -%}
                {{ exceptions.raise_compiler_error("ON_RUN_START_CHECK_NOT_PASSED: Data already exported. DBT Run aborted.") }}
            {% else -%}
                {{ log("No data found in " ~ table ~ " for current day and runtime region. Proceeding...", true) }}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
"""

before_and_after = """
{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into {{ target.schema }}.on_run_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    '{{ state }}',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )

{% endmacro %}
"""


hook = """
{% macro hook() %}
  select 1
{% endmacro %}
"""
